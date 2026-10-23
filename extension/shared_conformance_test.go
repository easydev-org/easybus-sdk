package extension

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/easydev-org/easybus-sdk/protocol"
)

// The shared cross-language tables.
//
// Each of these describes behaviour that is implemented once per language. They are read from
// the SAME files in every SDK, so a divergence fails a test here instead of surfacing as a
// message that silently never arrives.

func loadTable(t *testing.T, name string) map[string]any {
	t.Helper()
	path := filepath.Join("..", "conformance-cases", name)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return doc
}

func casesOf(doc map[string]any) []map[string]any {
	raw, _ := doc["cases"].([]any)
	out := make([]map[string]any, 0, len(raw))
	for _, c := range raw {
		out = append(out, c.(map[string]any))
	}
	return out
}

// TestSharedChannelMatching pins the subscription-matching rules.
func TestSharedChannelMatching(t *testing.T) {
	cs := casesOf(loadTable(t, "channel-matching.json"))
	if len(cs) == 0 {
		t.Fatal("empty table")
	}
	for i, c := range cs {
		pattern, _ := c["pattern"].(string)
		topic, _ := c["topic"].(string)
		want, _ := c["want"].(bool)
		if got := protocol.MatchSubject(pattern, topic); got != want {
			t.Errorf("case %d (%v): match(%q, %q) = %v, want %v",
				i, c["why"], pattern, topic, got, want)
		}
	}
}

// TestSharedChannelSubjects pins the build and parse round trip.
func TestSharedChannelSubjects(t *testing.T) {
	cs := casesOf(loadTable(t, "channel-subjects.json"))
	if len(cs) == 0 {
		t.Fatal("empty table")
	}
	for i, c := range cs {
		if b, ok := c["build"].(map[string]any); ok {
			tenant, _ := b["tenant"].(string)
			channel, _ := b["channel"].(string)
			topic, _ := b["topic"].(string)
			want, _ := c["want"].(string)
			got, err := protocol.ChChannel(tenant, channel, topic)
			if err != nil {
				t.Errorf("case %d (%v): unexpected error %v", i, c["why"], err)
				continue
			}
			if got != want {
				t.Errorf("case %d (%v): chChannel = %q, want %q", i, c["why"], got, want)
			}
			continue
		}
		if p, ok := c["parse"].(string); ok {
			tenant, channel, topic, ok2 := protocol.ParseChannelSubject(p)
			if c["want"] == nil {
				if ok2 {
					t.Errorf("case %d (%v): parse(%q) should be nil, got %q/%q/%q",
						i, c["why"], p, tenant, channel, topic)
				}
				continue
			}
			wantArr := c["want"].([]any)
			got := []string{tenant, channel, topic}
			for j := range wantArr {
				if got[j] != wantArr[j].(string) {
					t.Errorf("case %d (%v): parse(%q)[%d] = %q, want %v",
						i, c["why"], p, j, got[j], wantArr[j])
				}
			}
			continue
		}
		if r, ok := c["reject"].(map[string]any); ok {
			tenant, _ := r["tenant"].(string)
			channel, _ := r["channel"].(string)
			topic, _ := r["topic"].(string)
			if _, err := protocol.ChChannel(tenant, channel, topic); err == nil {
				t.Errorf("case %d (%v): chChannel(%q,%q,%q) should be refused",
					i, c["why"], tenant, channel, topic)
			}
		}
	}
}

// TestSharedProtocolSubjects pins every subject string.
func TestSharedProtocolSubjects(t *testing.T) {
	cs := casesOf(loadTable(t, "protocol-subjects.json"))
	if len(cs) == 0 {
		t.Fatal("empty table")
	}
	for i, c := range cs {
		fn, _ := c["fn"].(string)
		args, _ := c["args"].(map[string]any)
		want, _ := c["want"].(string)
		got := protocol.EvalSubject(fn, args)
		if got != want {
			t.Errorf("case %d (%s, %v): got %q, want %q", i, fn, c["why"], got, want)
		}
	}
}
