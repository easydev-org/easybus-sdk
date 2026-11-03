package denial

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/easydev-org/easybus-sdk/bus"
)

// A REFUSAL LIST IS ONLY USEFUL IF IT AGGREGATES.
//
// The same capability refused twenty times is ONE decision for the reader. Twenty rows would bury it,
// and a list that buries the thing it exists to surface is worse than no list.

// fakeKV is an in-memory bucket with the two properties that matter: a put replaces, a get returns.
type fakeKV struct {
	bus.Bus
	mu   sync.Mutex
	data map[string]string
	ttl  map[string]int64
}

func newFakeKV() *fakeKV { return &fakeKV{data: map[string]string{}, ttl: map[string]int64{}} }

func (f *fakeKV) KVPut(_ context.Context, _, key, value string, ttl int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[key] = value
	f.ttl[key] = ttl
	return nil
}

func (f *fakeKV) KVGet(_ context.Context, _, key string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.data[key], nil
}

func (f *fakeKV) KVDelete(_ context.Context, _, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.data, key)
	return nil
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func TestRecordAggregatesByCapability(t *testing.T) {
	kv := newFakeKV()
	r := NewRegistry(kv)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := r.Record(ctx, "alice", "s1", "sandbox", "exec", "sandbox-job-start"); err != nil {
			t.Fatal(err)
		}
	}
	// ONE KEY, because the reader decides about a capability once.
	if len(kv.data) != 1 {
		t.Fatalf("%d keys, want 1 — the refusals did not fold together", len(kv.data))
	}
	var rec Record
	if err := json.Unmarshal([]byte(kv.data[Key("alice", "s1", "sandbox", "exec")]), &rec); err != nil {
		t.Fatal(err)
	}
	if rec.Count != 3 {
		t.Fatalf("count = %d, want 3", rec.Count)
	}
}

// THE FIRST SIGHTING IS KEPT, NOT OVERWRITTEN.
//
// "When did this start" is the question a reader asks — and a record that moved its own start time on
// every repetition would make a week-old refusal look like it began a second ago.
func TestRecordKeepsTheOriginalFirstSeen(t *testing.T) {
	kv := newFakeKV()
	r := NewRegistry(kv)
	ctx := context.Background()
	key := Key("alice", "s1", "sandbox", "exec")

	old := time.Now().Add(-time.Hour).UnixMilli()
	kv.data[key] = mustJSON(Record{ExtID: "sandbox", Capability: "exec", FirstAt: old, LastAt: old, Count: 5})

	if err := r.Record(ctx, "alice", "s1", "sandbox", "exec", "sandbox-job-start"); err != nil {
		t.Fatal(err)
	}
	var rec Record
	if err := json.Unmarshal([]byte(kv.data[key]), &rec); err != nil {
		t.Fatal(err)
	}
	if rec.FirstAt != old {
		t.Fatalf("first sighting moved from %d to %d", old, rec.FirstAt)
	}
	if rec.Count != 6 {
		t.Fatalf("count = %d, want 6", rec.Count)
	}
	if rec.LastAt <= old {
		t.Fatal("last sighting did not advance")
	}
}

// THE WRITE IS A PUT, NOT A CREATE.
//
// A create reports an existing key as `(0, nil)` — success without a write — so the second refusal
// onward would vanish silently. That is the mistake the job registry made with its heartbeat.
func TestRecordWritesEveryTime(t *testing.T) {
	kv := newFakeKV()
	r := NewRegistry(kv)
	ctx := context.Background()
	key := Key("alice", "s1", "sandbox", "exec")

	for i := 1; i <= 2; i++ {
		if err := r.Record(ctx, "alice", "s1", "sandbox", "exec", "t"); err != nil {
			t.Fatal(err)
		}
		var rec Record
		if err := json.Unmarshal([]byte(kv.data[key]), &rec); err != nil {
			t.Fatal(err)
		}
		if rec.Count != i {
			t.Fatalf("after %d calls the count is %d — a write was skipped", i, rec.Count)
		}
	}
}

func TestKeySegmentsMatchTheListPrefix(t *testing.T) {
	// THE PREFIX ENDS IN `>`, because a capability key contains dots and `*` matches one token only.
	k := Key("alice", "s1", "sandbox", "exec")
	p := SessionPrefix("alice", "s1")
	if p[len(p)-1] != '>' {
		t.Fatalf("prefix %q must end in >, not *", p)
	}
	if len(k) <= len(p)-1 || k[:len(p)-1] != p[:len(p)-1] {
		t.Fatalf("a denial at %q is not visible under %q", k, p)
	}
}

func TestClearForgets(t *testing.T) {
	kv := newFakeKV()
	r := NewRegistry(kv)
	ctx := context.Background()

	if err := r.Record(ctx, "alice", "s1", "sandbox", "exec", "t"); err != nil {
		t.Fatal(err)
	}
	// A LIST THAT KEEPS COMPLAINING about a permission just granted teaches the reader it is noise.
	if err := r.Clear(ctx, "alice", "s1", "sandbox", "exec"); err != nil {
		t.Fatal(err)
	}
	if _, ok := kv.data[Key("alice", "s1", "sandbox", "exec")]; ok {
		t.Fatal("the record survived Clear")
	}
}

func TestTTLIsADay(t *testing.T) {
	if NewRegistry(nil).ttl != 24*time.Hour {
		t.Fatalf("ttl = %v, want 24h", NewRegistry(nil).ttl)
	}
}
