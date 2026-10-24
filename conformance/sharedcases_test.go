package conformance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/easydev-org/easybus-sdk/extension"
	"github.com/easydev-org/easybus-sdk/protocol"
)

// The shared cases describe behaviour every SDK must implement identically. Each
// SDK reads its own COPY of conformance-cases/, and the copy is pinned by a hash —
// so an SDK that has not been refreshed fails here instead of quietly asserting an
// older contract.

// casesDir is declared in broker_cases.go (the runner is compiled into the package), together
// with the search that finds it in a source tree OR inside the pinned-environment image.
// casesLock follows casesDir rather than a second hard-coded path: two paths for the same
// directory is the same defect the search exists to avoid.
var casesLock = filepath.Join(casesDir, "cases.lock")

// sharedCase is one language-neutral assertion.
type sharedCase struct {
	ID   string `json:"id"`
	Why  string `json:"why"`
	Kind string `json:"kind"` // "kv" | "constant" | "permission-matching" | "channel-matching" | "channel-subjects" | "protocol-subjects"
	KV   *struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	} `json:"kv"`
	Knobs map[string]struct {
		Type string `json:"type"`
	} `json:"knobs"`
	Expect *struct {
		Knob      string `json:"knob"`
		Value     any    `json:"value"`
		ValueType string `json:"valueType"`
	} `json:"expect"`
	Constant *struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	} `json:"constant"`
	// Cases carries the assertions for the pure-computation kinds. One field for all of them
	// because the shapes differ (grant/target, pattern/topic, build/parse, fn/args) and a
	// separate Go type per kind would duplicate what the JSON already declares.
	Cases []map[string]any `json:"cases"`
}

// TestSharedCases runs every case in the shared directory.
func TestSharedCases(t *testing.T) {
	cases := loadSharedCases(t)
	if len(cases) == 0 {
		t.Fatal("no shared cases found")
	}
	for _, c := range cases {
		c := c
		t.Run(c.ID, func(t *testing.T) {
			switch c.Kind {
			case "constant":
				runConstantCase(t, c)
			case "kv":
				runKVCase(t, c)
			case "permission-matching":
				runPermissionMatchingCase(t, c)
			case "channel-matching":
				runChannelMatchingCase(t, c)
			case "channel-subjects":
				runChannelSubjectsCase(t, c)
			case "protocol-subjects":
				runProtocolSubjectsCase(t, c)
			case "broker-fixture", "broker-cases":
				// Not a single assertion, and not a broker case either: these are the INPUTS to
				// the broker suite (the extension it builds and the calls it makes), which
				// conformance/broker_cases.go reads directly. They live in this directory so
				// every SDK reads the same bytes, but they have no runner of their own.
				t.Skip("consumed by the broker-cases runner, not a standalone case")
			default:
				t.Fatalf("unknown case kind %q", c.Kind)
			}
		})
	}
}

// TestSharedCasesAreCurrent fails when this SDK's copy has drifted from the lock.
//
// The copy is what the assertions run against, so a stale copy keeps passing while
// the real contract moved — the failure mode this whole mechanism exists to stop.
func TestSharedCasesAreCurrent(t *testing.T) {
	want := readTrimmed(t, casesLock)
	got := caseDigest(t)
	if got != want {
		t.Fatalf("conformance-cases/ does not match cases.lock\n  lock: %s\n  copy: %s\n"+
			"run conformance/cases/gen-lock.sh in the SDK repo that owns the change, "+
			"then copy conformance-cases/ into this one", want, got)
	}
}

func loadSharedCases(t *testing.T) []sharedCase {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(casesDir, "*.json"))
	if err != nil {
		t.Fatalf("glob cases: %v", err)
	}
	sort.Strings(paths)
	out := make([]sharedCase, 0, len(paths))
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		var c sharedCase
		if err := json.Unmarshal(raw, &c); err != nil {
			t.Fatalf("parse %s: %v", p, err)
		}
		if c.ID == "" {
			t.Fatalf("%s has no id", p)
		}
		out = append(out, c)
	}
	return out
}

// caseDigest hashes the case files the same way gen-lock.sh does, so the two can be
// compared. The NAME of each file is hashed with its contents: renaming a case is a
// change, and adding one cannot collide with editing another.
func caseDigest(t *testing.T) string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(casesDir, "*.json"))
	if err != nil {
		t.Fatalf("glob cases: %v", err)
	}
	sort.Strings(paths)
	h := sha256.New()
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		h.Write(raw)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func readTrimmed(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	s := string(raw)
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}

func runConstantCase(t *testing.T, c sharedCase) {
	t.Helper()
	if c.Constant == nil {
		t.Fatal("constant case has no constant block")
	}
	switch c.Constant.Name {
	case "configKVBucket":
		if protocol.ConfigKVBucket != c.Constant.Value {
			t.Fatalf("ConfigKVBucket = %q, want %q. %s",
				protocol.ConfigKVBucket, c.Constant.Value, c.Why)
		}
	default:
		t.Fatalf("unknown constant %q", c.Constant.Name)
	}
}

func runKVCase(t *testing.T, c sharedCase) {
	t.Helper()
	if c.KV == nil || c.Expect == nil {
		t.Fatal("kv case needs both a kv block and an expect block")
	}
	tenant, extID := "shared-t1", "shared-ext"
	key := subst(c.KV.Key, tenant, extID)

	specs := make(map[string]extension.ConfigSpec, len(c.Knobs))
	for name, k := range c.Knobs {
		specs[name] = extension.ConfigSpec{Type: k.Type}
	}

	agentBus, extBus, cleanup := freshPair(t)
	defer cleanup()

	ctx := context.Background()
	if err := agentBus.KVPut(ctx, protocol.ConfigKVBucket, key, c.KV.Value, 0); err != nil {
		t.Fatalf("seed kv: %v", err)
	}
	ext := extension.New(extBus, extension.Config{ID: extID, Version: "1", Config: specs})
	if err := ext.Serve(ctx); err != nil {
		t.Fatalf("serve: %v", err)
	}
	defer ext.Close()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if matchesCase(ext.GetConfig(tenant, c.Expect.Knob, ""), c.Expect.Value, c.Expect.ValueType) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s = %#v, want %#v (%s). %s",
		c.Expect.Knob, ext.GetConfig(tenant, c.Expect.Knob, ""), c.Expect.Value,
		c.Expect.ValueType, c.Why)
}

// matchesCase compares by the TYPE the case declares, not by Go's representation:
// a number arrives as float64 here, f64 in Rust, number in TypeScript, and the case
// states the intent rather than the encoding.
func matchesCase(got, want any, valueType string) bool {
	switch valueType {
	case "string":
		g, ok := got.(string)
		if !ok {
			return false
		}
		w, _ := want.(string)
		return g == w
	case "number":
		g, ok := toFloat(got)
		if !ok {
			return false
		}
		w, ok := toFloat(want)
		return ok && g == w
	case "bool":
		g, ok := got.(bool)
		if !ok {
			return false
		}
		w, _ := want.(bool)
		return g == w
	case "null":
		return got == nil
	}
	return false
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	}
	return 0, false
}

func subst(s, tenant, extID string) string {
	s = strings.ReplaceAll(s, "{tenant}", tenant)
	return strings.ReplaceAll(s, "{ext}", extID)
}
