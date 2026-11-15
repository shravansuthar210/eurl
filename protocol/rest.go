package protocol

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

type restHandler struct{}

func init() {
	Register(&restHandler{})
}

func (r *restHandler) Name() string { return "rest" }

func (r *restHandler) Execute(opts *RequestOptions) (*Response, error) {
	if opts.URL == "" {
		return nil, fmt.Errorf("rest: no URL given (use -u/--url)")
	}

	method := opts.Method
	if method == "" {
		if opts.Data != "" || opts.DataFile != "" {
			method = http.MethodPost
		} else {
			method = http.MethodGet
		}
	}

	var body io.Reader
	switch {
	case opts.DataFile != "":
		f, err := os.Open(opts.DataFile)
		if err != nil {
			return nil, fmt.Errorf("rest: reading data file: %w", err)
		}
		defer f.Close()
		body = f
	case opts.Data != "":
		body = bytes.NewBufferString(opts.Data)
	}

	req, err := http.NewRequest(method, opts.URL, body)
	if err != nil {
		return nil, fmt.Errorf("rest: building request: %w", err)
	}

	for _, h := range opts.Headers {
		parts := strings.SplitN(h, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("rest: bad header %q, want \"Key: Value\"", h)
		}
		req.Header.Add(strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]))
	}

	client := &http.Client{Timeout: opts.Timeout}
	if opts.Insecure {
		client.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	}

	if opts.Verbose {
		fmt.Fprintf(os.Stderr, "> %s %s\n", method, opts.URL)
		for _, h := range opts.Headers {
			fmt.Fprintf(os.Stderr, "> %s\n", h)
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("rest: request failed: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("rest: reading response: %w", err)
	}

	return &Response{
		StatusLine: resp.Status,
		Headers:    resp.Header,
		Body:       raw,
	}, nil
}
