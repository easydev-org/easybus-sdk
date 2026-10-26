package nats

import (
	"context"
	"testing"
	"time"

	"github.com/easydev-org/easybus-sdk/protocol"
)

// A lost CAS must be distinguishable from a failed CAS.
//
// `Renew` uses the return value to decide whether the lease was LOST (stop the turn) or whether
// the call merely FAILED (retry next tick). Returning (0, nil) for every error collapsed those
// two: a lease renewal that lost a race looked identical to a network blip, so the runtime could
// abort a perfectly healthy turn — or keep running one it had lost.
func TestKVCasDistinguishesConflictFromFailure(t *testing.T) {
	url := testBrokerURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	b, err := Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer b.Close()

	bucket := "casconfl"
	key := protocol.TenantKVKey("t1", protocol.SessionToken("s1"))

	// KVCreate creates the bucket as a side effect, which is how the real lease path works.
	rev1, err := b.KVCreate(ctx, bucket, key, "v1", 0)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if rev1 == 0 {
		t.Fatal("create returned rev 0, want the new revision")
	}

	// Stale revision: a lost race. Must be rev 0 with NO error — callers rely on the distinction.
	revStale, err := b.KVCas(ctx, bucket, key, "v2", rev1-1)
	if err != nil {
		t.Fatalf("a lost CAS reported an error (%v); callers cannot tell a lost race from a "+
			"broken connection", err)
	}
	if revStale != 0 {
		t.Fatalf("a lost CAS returned rev %d, want 0", revStale)
	}

	// The correct revision still works.
	rev2, err := b.KVCas(ctx, bucket, key, "v3", rev1)
	if err != nil {
		t.Fatalf("correct CAS: %v", err)
	}
	if rev2 <= rev1 {
		t.Fatalf("rev did not advance: %d -> %d", rev1, rev2)
	}
}

// An absent key is a miss, and a missing key must not look like a broken bucket.
func TestKVGetAbsentKeyIsAMiss(t *testing.T) {
	url := testBrokerURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	b, err := Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer b.Close()

	// Create the bucket first so the read is a MISS rather than "bucket not found" (which IS an
	// error worth reporting).
	if _, err := b.KVCreate(ctx, "misscheck", "seed", "x", 0); err != nil {
		t.Fatalf("seed bucket: %v", err)
	}
	v, err := b.KVGet(ctx, "misscheck", "no-such-key")
	if err != nil {
		t.Fatalf("an absent key reported an error (%v); callers take their default branch on a "+
			"miss and would never retry if a real failure looked the same", err)
	}
	if v != "" {
		t.Fatalf("got %q, want empty", v)
	}
}
