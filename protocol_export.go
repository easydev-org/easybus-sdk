package easybus

import "github.com/easydev-org/easybus-sdk/protocol"

// ArgInt/ArgFloat/ArgBool read tool arguments with defaults (ArgString lives in
// compat.go).
func ArgInt(args map[string]any, key string, def int64) int64 { return protocol.ArgInt(args, key, def) }
func ArgFloat(args map[string]any, key string, def float64) float64 {
	return protocol.ArgFloat(args, key, def)
}
func ArgBool(args map[string]any, key string, def bool) bool { return protocol.ArgBool(args, key, def) }
