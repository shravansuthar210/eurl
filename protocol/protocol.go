package protocol

import "time"

// RequestOptions holds every curl-like parameter collected from the CLI.
// Every protocol handler receives the same struct, so adding a new
// handler never requires touching the flag-parsing code in main.go.
type RequestOptions struct {
	URL      string
	Method   string
	Headers  []string // raw "Key: Value" strings
	Data     string
	DataFile string
	Output   string
	Insecure bool
	Verbose  bool
	Timeout  time.Duration

	// Topic is reused across non-HTTP handlers as "the named thing
	// this call targets": queue name for -rabbitmq, topic name for
	// -kafka, fully qualified "package.Service/Method" for -grpc
	// (set via -topic/-queue/-rpc, all three map to this one field).
	// HTTP-style handlers (-rest, -graphql) ignore it.
	Topic string
}

// timeoutOrDefault gives every handler the same fallback timeout so
// broker handlers (which don't go through net/http's own default)
// behave consistently with -rest.
func timeoutOrDefault(d time.Duration) time.Duration {
	if d <= 0 {
		return 30 * time.Second
	}
	return d
}

// Response is the normalized result any handler returns, regardless of
// which wire protocol it actually spoke underneath.
type Response struct {
	StatusLine string
	Headers    map[string][]string
	Body       []byte
}

// Handler is the extension point of eurl. To add a new protocol
// (graphql, grpc, soap, a raw TCP probe, whatever):
//
//  1. create internal/protocol/<name>.go
//  2. implement this interface
//  3. call Register(&yourHandler{}) from an init() in that file
//
// main.go auto-discovers it and a matching "-<name>" CLI flag appears
// with no other code changes.
type Handler interface {
	// Name is the protocol identifier, e.g. "rest". It becomes the
	// CLI flag: -rest, -graphql, -grpc, ...
	Name() string

	// Execute performs the call described by opts and returns a
	// normalized Response.
	Execute(opts *RequestOptions) (*Response, error)
}
