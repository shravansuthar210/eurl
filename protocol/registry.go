package protocol

import "fmt"

var registry = map[string]Handler{}

// Register makes a Handler available under Handler.Name(). Protocol
// packages call this from their own init(), so importing the protocol
// package is all that's needed to plug it in.
func Register(h Handler) {
	registry[h.Name()] = h
}

// Get looks up a registered handler by name.
func Get(name string) (Handler, error) {
	h, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("unknown protocol %q (available: %v)", name, Names())
	}
	return h, nil
}

// Names returns every registered protocol name, used both to build the
// CLI's dynamic flag set and for error messages.
func Names() []string {
	names := make([]string, 0, len(registry))
	for n := range registry {
		names = append(names, n)
	}
	return names
}
