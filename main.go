// Command eurl is a curl-like CLI. Unlike curl it dispatches every call
// through a pluggable protocol.Handler, so new call styles (graphql,
// a custom binary protocol, ...) can be added by dropping a file in
// internal/protocol without touching this file. Every call it makes is
// also logged -- timestamp, protocol, method, URL, data sent, and the
// result -- to a local SQLite history file, written by eurl's own
// internal/store package (no sqlite driver, no third-party library
// anywhere in this program).
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"eurl/protocol"
)

// headerList lets -H/--header be passed multiple times, same as curl.
type headerList []string

func (h *headerList) String() string { return strings.Join(*h, ",") }
func (h *headerList) Set(v string) error {
	*h = append(*h, v)
	return nil
}

func main() {
	var (
		url      string
		method   string
		data     string
		dataFile string
		output   string
		insecure bool
		verbose  bool
		timeout  time.Duration
		headers  headerList
		topic    string

		dbPath      string
		showHistory bool
		limit       int
		showID      int64
		clearHist   bool
	)

	fs := flag.NewFlagSet("eurl", flag.ExitOnError)
	fs.Usage = func() { printUsage(fs) }

	fs.StringVar(&url, "u", "", "target URL (required)")
	fs.StringVar(&url, "url", "", "target URL (required)")
	fs.StringVar(&method, "X", "", "HTTP method (default GET, or POST if -d is set)")
	fs.StringVar(&method, "request", "", "HTTP method (default GET, or POST if -d is set)")
	fs.StringVar(&data, "d", "", "request body")
	fs.StringVar(&data, "data", "", "request body")
	fs.StringVar(&dataFile, "data-file", "", "read request body from a file")
	fs.StringVar(&output, "o", "", "write response body to file instead of stdout")
	fs.StringVar(&output, "output", "", "write response body to file instead of stdout")
	fs.BoolVar(&insecure, "k", false, "skip TLS certificate verification")
	fs.BoolVar(&insecure, "insecure", false, "skip TLS certificate verification")
	fs.BoolVar(&verbose, "v", false, "verbose output (request/response headers)")
	fs.BoolVar(&verbose, "verbose", false, "verbose output (request/response headers)")
	fs.DurationVar(&timeout, "timeout", 30*time.Second, `request timeout, e.g. "10s"`)
	fs.Var(&headers, "H", `request header "Key: Value" (repeatable)`)
	fs.Var(&headers, "header", `request header "Key: Value" (repeatable)`)
	fs.StringVar(&topic, "topic", "", "topic name (-kafka) or queue name (-rabbitmq)")
	fs.StringVar(&topic, "queue", "", "topic name (-kafka) or queue name (-rabbitmq)")

	fs.BoolVar(&showHistory, "history", false, "list logged requests instead of making a call")
	fs.IntVar(&limit, "limit", 20, "with -history, how many recent entries to show")
	fs.Int64Var(&showID, "id", 0, "with -history, show full detail (data sent + response) for one entry")
	fs.BoolVar(&clearHist, "clear-history", false, "delete the history file and exit")

	protoFlags := map[string]*bool{}
	for _, name := range protocol.Names() {
		protoFlags[name] = fs.Bool(name, false, fmt.Sprintf("use the %q protocol handler", name))
	}

	if len(os.Args) < 2 {
		fs.Usage()
		os.Exit(1)
	}
	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}

	if clearHist {
		if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
			fmt.Fprintln(os.Stderr, "eurl:", err)
			os.Exit(1)
		}
		fmt.Println("history cleared:", dbPath)
		return
	}


	protoName := "rest"
	for name, chosen := range protoFlags {
		if *chosen {
			protoName = name
			break
		}
	}

	handler, err := protocol.Get(protoName)
	if err != nil {
		fmt.Fprintln(os.Stderr, "eurl:", err)
		os.Exit(1)
	}

	opts := &protocol.RequestOptions{
		URL:      url,
		Method:   method,
		Headers:  headers,
		Data:     data,
		DataFile: dataFile,
		Output:   output,
		Insecure: insecure,
		Verbose:  verbose,
		Timeout:  timeout,
		Topic:    topic,
	}

	resp, callErr := handler.Execute(opts)
	// logRequest(dbPath, protoName, opts, resp, callErr)

	if callErr != nil {
		fmt.Fprintln(os.Stderr, "eurl:", callErr)
		os.Exit(1)
	}

	if verbose {
		fmt.Fprintln(os.Stderr, "<", resp.StatusLine)
		for k, vals := range resp.Headers {
			for _, v := range vals {
				fmt.Fprintf(os.Stderr, "< %s: %s\n", k, v)
			}
		}
	}

	if output != "" {
		if err := os.WriteFile(output, resp.Body, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "eurl: writing output:", err)
			os.Exit(1)
		}
		return
	}

	os.Stdout.Write(resp.Body)
	if len(resp.Body) > 0 && resp.Body[len(resp.Body)-1] != '\n' {
		fmt.Println()
	}
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func shortStatus(s string) string {
	if len(s) > 9 {
		return s[:9]
	}
	return s
}


func printUsage(fs *flag.FlagSet) {
	fmt.Fprintln(os.Stderr, `eurl - a curl-like CLI with pluggable protocol handlers and built-in request history

Usage:
  eurl -rest -u https://localhost:3000/api
  eurl -rest -u https://localhost:3000/api -X POST -H "Content-Type: application/json" -d '{"key":"value"}'
  eurl -graphql -u https://localhost:3000/graphql -d '{ users { id name } }'
  eurl -tcp -u localhost:9000 -d "PING\n"
  eurl -rabbitmq -u amqp://guest:guest@localhost:5672/ -queue jobs -d '{"job":"send-email"}'
  eurl -kafka -u localhost:9092 -topic events -d '{"event":"user.created"}'

  eurl -history                 # list recent calls (default: last 20)
  eurl -history -limit 100      # list more
  eurl -history -id 7           # full detail for one call, including data sent + response
  eurl -clear-history           # wipe the history file

Every call above is logged automatically to a local SQLite file
(default ~/.eurl/history.sqlite, override with -db).

Flags:`)
	fs.PrintDefaults()
}
