package protocol

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestGenerateGoldenValues rewrites the `want` field of every protocol-subjects case from the
// reference implementation.
//
// The golden table exists to pin behaviour every SDK must reproduce. Writing it BY HAND would
// only record what one person believed the subjects were — so it is generated here, from Go,
// and the other three SDKs then have to agree with it. Run with:
//
//	EASYBUS_WRITE_GOLDEN=1 go test ./protocol -run TestGenerateGoldenValues
func TestGenerateGoldenValues(t *testing.T) {
	if os.Getenv("EASYBUS_WRITE_GOLDEN") != "1" {
		t.Skip("set EASYBUS_WRITE_GOLDEN=1 to regenerate")
	}
	path := filepath.Join("..", "conformance-cases", "protocol-subjects.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse: %v", err)
	}
	cases, _ := doc["cases"].([]any)
	for _, c := range cases {
		m := c.(map[string]any)
		fn, _ := m["fn"].(string)
		args, _ := m["args"].(map[string]any)
		m["want"] = EvalSubject(fn, args)
	}
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, append(out, '\n'), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	t.Log("regenerated", path)
}
