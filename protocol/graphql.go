package protocol

import (
	"encoding/json"
	"fmt"
)

// graphqlHandler shows how cheap it is to bolt on a new protocol: it
// wraps restHandler, forces POST + a JSON content-type header, and
// wraps opts.Data as a {"query": "..."} payload if it isn't already
// JSON. Register() below is the only thing main.go needs to see it.
type graphqlHandler struct {
	rest *restHandler
}

func init() {
	Register(&graphqlHandler{rest: &restHandler{}})
}

func (g *graphqlHandler) Name() string { return "graphql" }

func (g *graphqlHandler) Execute(opts *RequestOptions) (*Response, error) {
	call := *opts // shallow copy so we don't mutate the caller's opts
	call.Method = "POST"

	hasContentType := false
	for _, h := range call.Headers {
		if len(h) >= 12 && (h[:12] == "Content-Type" || h[:12] == "content-type") {
			hasContentType = true
			break
		}
	}
	if !hasContentType {
		call.Headers = append(append([]string{}, call.Headers...), "Content-Type: application/json")
	}

	if call.Data != "" && !json.Valid([]byte(call.Data)) {
		payload, err := json.Marshal(map[string]string{"query": call.Data})
		if err != nil {
			return nil, fmt.Errorf("graphql: wrapping query: %w", err)
		}
		call.Data = string(payload)
	}

	return g.rest.Execute(&call)
}
