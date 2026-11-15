# eurl

A curl-like CLI, written in Go, **with zero third-party dependencies**.
Every call is dispatched through a pluggable protocol `Handler`, and
every call is logged -- timestamp, method, URL, data sent, and the
result -- to a local SQLite file that eurl's own code writes directly,
byte by byte, without a sqlite driver.

Handlers today: `rest`, `graphql`, `tcp`, `rabbitmq`, `kafka`. Adding
another never requires touching `main.go`.

## Build

Requires Go 1.21+ (not installed in this sandbox, so build it on your
own machine). `go.mod` has no `require` block -- nothing to fetch.

```bash
cd eurl
go build -o eurl ./cmd/eurl
```

## Usage

```bash
eurl -rest -u https://localhost:3000/api
eurl -rest -u https://localhost:3000/api -X POST -H "Content-Type: application/json" -d '{"key":"value"}'
eurl -graphql -u https://localhost:3000/graphql -d '{ users { id name } }'
eurl -tcp -u localhost:9000 -d "PING\n"
eurl -rabbitmq -u amqp://guest:guest@localhost:5672/ -queue jobs -d '{"job":"send-email"}'
eurl -kafka -u localhost:9092 -topic events -d '{"event":"user.created"}'

# request management -- every call above gets logged automatically
eurl -history                 # last 20 calls
eurl -history -limit 100      # more
eurl -history -id 7           # full detail: headers, data sent, response
eurl -clear-history           # wipe the log
```

If no protocol flag is given, `rest` is the default.

### Flags

| Flag | Meaning |
|---|---|
| `-u`, `-url` | target URL / broker address |
| `-X`, `-request` | HTTP method (rest/graphql only) |
| `-H`, `-header` | header, repeatable (rest/graphql only) |
| `-d`, `-data` | request body / message payload |
| `-data-file` | read body from a file |
| `-o`, `-output` | write body to a file |
| `-k`, `-insecure` | skip TLS verification |
| `-v`, `-verbose` | print headers to stderr |
| `-timeout` | request/connect timeout, e.g. `10s` |
| `-topic`, `-queue` | topic (kafka) / queue (rabbitmq) |
| `-db` | history file path (default `~/.eurl/history.sqlite`) |
| `-history`, `-limit`, `-id` | list / inspect past requests |
| `-clear-history` | delete the history file |

## What "no library" means here

`go.mod` pulls in nothing beyond the Go standard library. That has
real consequences for two of the handlers, worth knowing before you
rely on them:

- **`-rabbitmq`** hand-rolls AMQP 0-9-1 framing over a raw
  `net.Conn`: connection handshake, channel open, an optional durable
  `Queue.Declare`, and `Basic.Publish` to the default exchange. Not
  implemented: TLS, SASL mechanisms other than PLAIN, publisher
  confirms, and a graceful close handshake -- the socket is just
  closed after publishing, which brokers tolerate for a one-shot
  publish (they'll log a connection reset, nothing more).

- **`-kafka`** hand-rolls the *legacy* Produce API, version 0 --
  the old fixed-width message format with a plain CRC32 checksum
  (`hash/crc32` from the standard library), not the newer RecordBatch
  format (which needs zigzag varints and CRC32C). It's a deliberately
  simpler, lower-risk target to implement correctly by hand, and
  brokers still accept it for backward compatibility. Not implemented:
  consuming, compression, TLS/SASL, and cluster metadata discovery --
  `-u` is assumed to be the partition-0 leader directly, true for a
  typical single-broker dev/test setup.

Both of these were written from the public wire-protocol specs, by
hand, with **no way to test them against a live broker in this
environment** (no network, no Go toolchain here). Treat them as a
solid starting point, not something to point at production without
trying them against a real RabbitMQ/Kafka instance first.

**`-grpc` was dropped** from the earlier version of this project for
the same reason, taken further: a correct from-scratch gRPC client
needs a hand-rolled HTTP/2 client (its own large spec: HPACK header
compression, frame types, flow control) underneath the protobuf
wire codec, which is a much bigger undertaking than AMQP or Kafka's
plain-TCP framing. Happy to build that out as a dedicated follow-up if
you want it -- it's a real project on its own.

`-rest`, `-graphql`, and `-tcp` use only `net/http` and `net`, so
there was nothing to change there.

## The SQLite history file

`internal/store/sqlite.go` writes a real, valid SQLite3 database file
by hand -- page headers, b-tree cells, varints, the record format --
following the public file format spec
(https://www.sqlite.org/fileformat2.html). You can open it with the
`sqlite3` CLI or DB Browser for SQLite:

```bash
sqlite3 ~/.eurl/history.sqlite "select id, ts, protocol, method, url, status from requests"
```

It's a deliberately narrow subset of the format:

- one fixed table, `requests` (`id, ts, protocol, method, url,
  headers, data_sent, status, response`), schema baked in at creation
- **insert (append) and full-table read only** -- there's no SQL
  parser in here, no UPDATE/DELETE, no indexes, no WHERE clause.
  `-history` does a full scan and filters/sorts in Go.
- a b-tree at most 2 levels deep (a root leaf, or a root interior page
  pointing at leaf pages). That comfortably covers tens of thousands
  of logged requests; if the root interior page itself ever fills up,
  `Insert` returns an error rather than silently corrupting the file
  (a real 3-level b-tree was out of scope here).
- every field is length-capped (URL 500, headers 500, data
  sent/response 800, status 300 bytes, truncated with a marker beyond
  that) so every record is guaranteed to fit in one 4KB page -- there's
  no overflow-page support.
- no rollback journal or WAL, so this isn't crash-safe in the way a
  real SQLite write transaction is. Fine for a local request log,
  not a guarantee to lean on for anything you can't afford to lose.

If you outgrow this (need real SQL queries, concurrent writers, etc.),
the file is completely standard -- point `database/sql` and any real
sqlite driver at the same path and it'll just work; you're not locked
into this hand-rolled writer.

## Architecture

```
cmd/eurl/main.go              flag parsing, dispatch, history logging/listing
internal/protocol/
  protocol.go                  Handler interface + RequestOptions/Response
  registry.go                  Register()/Get()/Names()
  rest.go                       plain HTTP(S) ("rest")
  graphql.go                    wraps rest, forces POST + JSON ("graphql")
  tcp.go                        raw TCP send/receive ("tcp")
  rabbitmq.go                   hand-rolled AMQP 0-9-1 publisher ("rabbitmq")
  kafka.go                      hand-rolled Kafka Produce v0 ("kafka")
internal/store/
  sqlite.go                     hand-rolled SQLite file reader/writer
```

## Adding a new protocol

1. Create `internal/protocol/<name>.go`.
2. Implement the `Handler` interface:
   ```go
   type myHandler struct{}

   func init() { Register(&myHandler{}) }

   func (h *myHandler) Name() string { return "myproto" }

   func (h *myHandler) Execute(opts *RequestOptions) (*Response, error) {
       // do the call, return a normalized *Response
   }
   ```
3. Rebuild. `eurl -myproto -u ...` now works, gets a CLI flag
   automatically, and gets logged to history automatically -- no other
   file changes.
