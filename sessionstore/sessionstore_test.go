package sessionstore

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/easydev-org/easybus-sdk/bus"
)

// THIS PACKAGE EXISTS BECAUSE TWO LISTS MADE THE SAME TWO MISTAKES.
//
// So its tests are about those mistakes, not about JSON round-tripping. Each one below corresponds to
// a bug that shipped: a wildcard that matched one token, and a write that stopped after the first.

// fakeKV records every call, so "did the write happen" is a number rather than an inference.
type fakeKV struct {
	bus.Bus
	mu      sync.Mutex
	data    map[string]string
	creates int
	puts    int
}

func newFakeKV() *fakeKV { return &fakeKV{data: map[string]string{}} }

func (f *fakeKV) KVPut(_ context.Context, _, key, value string, _ int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts++
	f.data[key] = value
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

// KVCreate mimics the real bus: an existing key answers (0, nil) — success without a write. That is
// exactly why Put must never be built from Create.
func (f *fakeKV) KVCreate(_ context.Context, _, key, value string, _ int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates++
	if _, ok := f.data[key]; ok {
		return 0, nil
	}
	f.data[key] = value
	return 1, nil
}

// PUT WRITES EVERY TIME, EVEN WHEN THE KEY EXISTS.
//
// A create reports an existing key as `(0, nil)` — the normal losing side of a claim race — so
// "create, then put on failure" never reaches the put. For a heartbeat that means the entry is written
// once and never renewed: jobs appeared in the list and were marked lost while still running.
//
// THIS TEST FAILS IF SOMEBODY REINTRODUCES THE OPTIMISATION, which looks like an optimisation.
func TestPutWritesEvenWhenTheKeyExists(t *testing.T) {
	kv := newFakeKV()
	s := New(kv)
	ctx := context.Background()
	key := Key("alice", "s1", "x")

	for i := 1; i <= 3; i++ {
		if err := s.Put(ctx, "b", key, map[string]int{"n": i}, time.Minute); err != nil {
			t.Fatal(err)
		}
		if kv.puts != i {
			t.Fatalf("after %d puts the store has written %d times", i, kv.puts)
		}
	}
	// AND IT NEVER CREATES: a create is for deciding who wins a race, and writing a list is not one.
	if kv.creates != 0 {
		t.Fatalf("Put created the key %d times", kv.creates)
	}
}

// THE PREFIX MATCHES THE WHOLE REST OF THE KEY.
//
// NATS wildcards are per TOKEN: `*` is one, `>` is the remaining any number. A list whose ids contain
// dots — `sandbox-job.392f82df` — is therefore invisible to a `*` prefix, and the failure is silent:
// the entries are written, the watch is live, the list is empty.
func TestPrefixMatchesAMultiSegmentKey(t *testing.T) {
	p := Prefix("alice", "s1")
	if !strings.HasSuffix(p, ".>") {
		t.Fatalf("prefix %q must end in .>, not a single-token wildcard", p)
	}
	// A KEY WITH DOTS IN ITS TAIL, which is the case `*` could not reach.
	k := Key("alice", "s1", "sandbox-job", "abc123")
	if !strings.HasPrefix(k, strings.TrimSuffix(p, ">")) {
		t.Fatalf("a key at %q is outside %q", k, p)
	}
}

func TestKeySegmentsAreOrderedGeneralToSpecific(t *testing.T) {
	// One conversation lists its own with a single wildcard, which only works in this order.
	if got, want := Key("alice", "s1", "ext", "cap"), "t.alice.s.s1.ext.cap"; got != want {
		t.Fatalf("Key = %q, want %q", got, want)
	}
	if got, want := Prefix("alice", "s1"), "t.alice.s.s1.>"; got != want {
		t.Fatalf("Prefix = %q, want %q", got, want)
	}
}

// A SESSION NAME WITH A DOT MUST NOT SPLIT THE KEY.
//
// `a.b` written unescaped becomes `t.alice.s.a.b.<id>`, and a reader splitting on dots would take the
// session as `a` and the id as `b.<id>` — the write and the parse disagreeing about where the session
// ends. The entry exists and is simply never found, which is the same silent shape as the `*` wildcard
// bug: nothing errors, the list is short by one.
//
// `protocol` already escaped this for subscription keys and had a test for it. This code did not, which
// is what 「two implementations of one shape」 costs.
func TestKeyEscapesTheSessionName(t *testing.T) {
	plain := Key("alice", "a.b", "id1")
	if plain == "t.alice.s.a.b.id1" {
		t.Fatal("a dotted session name gained a segment; the read side would disagree")
	}
	// THE SEGMENT COUNT IS WHAT MATTERS: four separator characters for t / tenant / s / session.
	if got := strings.Count(plain, "."); got != 4 {
		t.Fatalf("key %q has %d dots, want 4 — the session escaped into more than one token", plain, got)
	}
	// AND AN ORDINARY NAME IS LEFT ALONE, so existing keys do not move.
	if got := Key("alice", "s1", "id1"); got != "t.alice.s.s1.id1" {
		t.Fatalf("an ordinary key changed shape: %q", got)
	}
}

// AN ABSENT KEY IS NOT AN ERROR, so a caller does not have to tell "missing" from "broken".
func TestGetReportsAbsenceWithoutAnError(t *testing.T) {
	s := New(newFakeKV())
	var v map[string]int
	ok, err := s.Get(context.Background(), "b", Key("a", "s", "nope"), &v)
	if err != nil {
		t.Fatalf("absent key returned an error: %v", err)
	}
	if ok {
		t.Fatal("absent key reported as present")
	}
}

func TestPutThenGetRoundTrips(t *testing.T) {
	s := New(newFakeKV())
	ctx := context.Background()
	key := Key("alice", "s1", "x")
	want := map[string]any{"id": "x", "count": float64(2)}

	if err := s.Put(ctx, "b", key, want, time.Minute); err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	ok, err := s.Get(ctx, "b", key, &got)
	if err != nil || !ok {
		t.Fatalf("Get = %v, %v", ok, err)
	}
	if got["id"] != "x" || got["count"] != float64(2) {
		t.Fatalf("round trip lost data: %#v", got)
	}
}
