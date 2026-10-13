// Package easybus re-exports the protocol helpers so consumers can use
// a single import for generated types and channel/argument helpers.
package easybus

import "github.com/easydev-org/easybus-sdk/protocol"

// ArgString reads a string tool argument (missing/typed wrong = "").
func ArgString(args map[string]any, key string) string { return protocol.ArgString(args, key) }

// ArgInt reads a numeric tool argument with a default.
func ArgInt(args map[string]any, key string, def int64) int64 {
	return protocol.ArgInt(args, key, def)
}

// ArgFloat reads a float tool argument with a default.
func ArgFloat(args map[string]any, key string, def float64) float64 {
	return protocol.ArgFloat(args, key, def)
}

// ArgBool reads a boolean tool argument with a default.
func ArgBool(args map[string]any, key string, def bool) bool {
	return protocol.ArgBool(args, key, def)
}
