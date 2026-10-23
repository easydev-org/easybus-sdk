package protocol

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	easybusv1 "github.com/easydev-org/easybus-sdk/gen/easybus/v1"
)

// The committed generated code must match the committed proto.
//
// Go commits `gen/`, so nothing at build time forces the two to agree: editing the proto and
// forgetting `buf generate` leaves code that compiles, passes every behavioural test, and
// silently omits fields — a field the generated code does not know is dropped on the wire.
//
// This check is FIELD-level, not message-level. A message-level check misses the case that
// actually happened: `ExtensionTool` already existed and gained `requires`. The message was
// present, so "is every message there" answered yes while the field was absent.
func TestGeneratedCodeMatchesProto(t *testing.T) {
	protoPath := filepath.Join("..", "proto", "easybus", "v1", "protocol.proto")
	raw, err := os.ReadFile(protoPath)
	if err != nil {
		t.Fatalf("read proto: %v", err)
	}

	type msg struct {
		name   string
		fields []string
	}
	var msgs []msg
	var cur *msg
	depth := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "message ") && strings.HasSuffix(line, "{") {
			name := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(line, "message "), "{"))
			msgs = append(msgs, msg{name: name})
			cur = &msgs[len(msgs)-1]
			depth = 1
			continue
		}
		if cur == nil {
			continue
		}
		if strings.HasSuffix(line, "{") {
			depth++
			continue
		}
		if strings.HasPrefix(line, "}") {
			depth--
			if depth == 0 {
				cur = nil
			}
			continue
		}
		if !strings.Contains(line, "=") || !strings.HasSuffix(line, ";") {
			continue
		}
		lhs := strings.TrimSpace(strings.SplitN(line, "=", 2)[0])
		parts := strings.Fields(lhs)
		if len(parts) < 2 {
			continue
		}
		cur.fields = append(cur.fields, parts[len(parts)-1])
	}
	if len(msgs) == 0 {
		t.Fatal("parsed no messages from the proto — the parser is broken, not the code")
	}

	descs := easybusv1.File_easybus_v1_protocol_proto.Messages()
	byName := make(map[string]bool, descs.Len())
	fields := make(map[string]map[string]bool, descs.Len())
	for i := 0; i < descs.Len(); i++ {
		m := descs.Get(i)
		byName[string(m.Name())] = true
		set := make(map[string]bool, m.Fields().Len())
		for j := 0; j < m.Fields().Len(); j++ {
			set[string(m.Fields().Get(j).Name())] = true
		}
		fields[string(m.Name())] = set
	}

	var missing []string
	for _, m := range msgs {
		if !byName[m.name] {
			missing = append(missing, m.name+" (whole message)")
			continue
		}
		for _, f := range m.fields {
			if !fields[m.name][f] {
				missing = append(missing, m.name+"."+f)
			}
		}
	}
	if len(missing) > 0 {
		t.Fatalf("generated code is stale; run `buf generate`. Missing: %v", missing)
	}
}
