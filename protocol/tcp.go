package protocol

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

// tcpHandler is a netcat-style handler: dial host:port, write the
// payload, half-close the write side, and read whatever comes back
// until the peer closes the connection. It's intentionally simple —
// there's no framing/protocol logic here, just raw bytes in, raw
// bytes out. A real binary protocol on top of TCP should be its own
// handler that does its own framing and calls net.Dial itself.
type tcpHandler struct{}

func init() {
	Register(&tcpHandler{})
}

func (t *tcpHandler) Name() string { return "tcp" }

func (t *tcpHandler) Execute(opts *RequestOptions) (*Response, error) {
	if opts.URL == "" {
		return nil, fmt.Errorf("tcp: no address given (use -u/--url host:port)")
	}
	addr := strings.TrimPrefix(strings.TrimPrefix(opts.URL, "tcp://"), "//")

	conn, err := net.DialTimeout("tcp", addr, timeoutOrDefault(opts.Timeout))
	if err != nil {
		return nil, fmt.Errorf("tcp: dial %s: %w", addr, err)
	}
	defer conn.Close()

	if opts.Verbose {
		fmt.Fprintf(os.Stderr, "> connected to %s\n", addr)
	}

	payload := opts.Data
	if payload == "" && opts.DataFile != "" {
		raw, err := os.ReadFile(opts.DataFile)
		if err != nil {
			return nil, fmt.Errorf("tcp: reading data file: %w", err)
		}
		payload = string(raw)
	}

	if payload != "" {
		if _, err := conn.Write([]byte(payload)); err != nil {
			return nil, fmt.Errorf("tcp: write: %w", err)
		}
	}

	// Half-close the write side so peers that read-until-EOF (like a
	// simple echo server) know we're done sending.
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.CloseWrite()
	}

	raw, err := io.ReadAll(conn)
	if err != nil {
		return nil, fmt.Errorf("tcp: read: %w", err)
	}

	return &Response{
		StatusLine: fmt.Sprintf("connected %s", addr),
		Headers:    map[string][]string{},
		Body:       raw,
	}, nil
}
