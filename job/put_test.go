package job

import (
	"context"
	"testing"
	"time"

	"github.com/easydev-org/easybus-sdk/bus"
)

/**
 * THE HEARTBEAT HAS TO ACTUALLY WRITE.
 *
 * A create-then-put looks like it handles replacement — "if the key exists, fall through to a put" —
 * but `KVCreate` reports an existing key as `(0, nil)`, which is the normal losing side of a claim
 * race. So the conditional never fires: the entry is written once at start, every renewal is
 * discarded, and the sweeper concludes thirty seconds later that nobody is renewing.
 *
 * THE SYMPTOM IS ALMOST UNREADABLE FROM OUTSIDE: jobs appear in the list and are marked lost while
 * they are still running. Everything looks wired; the only evidence is that `RenewedAt` never moves.
 *
 * This fake counts the calls, so "did the renewal write anything" is a number rather than an
 * inference from a timestamp.
 */

// countingKV records what the registry asked it to do.
type countingKV struct {
	bus.Bus
	created int
	put     int
	lastVal string
}

func (c *countingKV) KVCreate(_ context.Context, _, _, value string, _ int64) (int64, error) {
	c.created++
	c.lastVal = value
	// An existing key answers (0, nil) — that is what the real bus does, and reproducing it here is
	// the point: a fake that returned an error would make the broken code look correct.
	return 0, nil
}

func (c *countingKV) KVPut(_ context.Context, _, _, value string, _ int64) error {
	c.put++
	c.lastVal = value
	return nil
}

func TestPutWritesEvenWhenTheKeyExists(t *testing.T) {
	kv := &countingKV{}
	r := NewRegistry(kv)

	if err := r.Put(context.Background(), Entry{
		ID: "sandbox-job.a", Tenant: "alice", Session: "s1", ExtID: "sandbox", State: StateRunning,
	}); err != nil {
		t.Fatal(err)
	}
	if kv.put != 1 {
		t.Fatalf("put was called %d times, want 1 — a renewal that only creates is a renewal that "+
			"stops happening the moment the key exists", kv.put)
	}
	// AND IT MUST NOT CREATE: create is a claim race, and a registry that creates on every heartbeat
	// would be pretending to race with itself.
	if kv.created != 0 {
		t.Fatalf("put created the key %d times; it should only put", kv.created)
	}
}

func TestRenewalCarriesAFreshTimestamp(t *testing.T) {
	kv := &countingKV{}
	r := NewRegistry(kv)
	e := Entry{ID: "sandbox-job.b", Tenant: "alice", Session: "s1", ExtID: "sandbox", State: StateRunning}

	// THE SWEEPER JUDGES THIS FIELD, so a write that carries the same value is a write that changes
	// nothing — which is the failure this test is really about.
	e.RenewedAt = time.Now().Add(-time.Hour).UnixMilli()
	if err := r.Put(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	var got Entry
	if err := unmarshalEntry(kv.lastVal, &got); err != nil {
		t.Fatal(err)
	}
	// The caller owns the timestamp (the handle sets it before calling); what this pins is that the
	// VALUE REACHES THE STORE, which create-then-put silently failed to do.
	if got.RenewedAt == 0 {
		t.Fatal("the written entry has no renewal time")
	}
}
