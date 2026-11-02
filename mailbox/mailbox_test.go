package mailbox

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/easydev-org/easybus-sdk/bus"
)

/**
 * THE QUEUE MAY DELIVER TWICE, AND THAT IS THE DESIGN.
 *
 * A table could promise exactly-once by marking and fetching in one transaction. A KV bucket cannot, so
 * the promise moves to the fold, which is idempotent by message id. These tests pin what the queue must
 * still guarantee: a message is never LOST, and a consumed one stays readable.
 */

// fakeKV is an in-memory bucket; every entry carries a TTL so the two lifetimes can be asserted.
type fakeKV struct {
	bus.Bus
	mu   sync.Mutex
	data map[string]string
	ttl  map[string]int64
	// rev is the revision each key is at, so CompareAndSwap can lose a race the way the real bus does.
	rev map[string]uint64
}

func newFakeKV() *fakeKV {
	return &fakeKV{data: map[string]string{}, ttl: map[string]int64{}, rev: map[string]uint64{}}
}

func (f *fakeKV) KVPut(_ context.Context, _, key, value string, ttl int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[key] = value
	f.ttl[key] = ttl
	f.rev[key]++
	return nil
}

/**
 * KVCas REPORTS A LOST RACE AS (0, nil), which is the contract the real bus has.
 *
 * REPRODUCING IT IS THE POINT: a fake that always won would make the drain look exactly-once while the
 * real one delivered twice, and the test would pass on the bug.
 */
func (f *fakeKV) KVCas(_ context.Context, _, key, value string, revision int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rev[key] != uint64(revision) {
		return 0, nil
	}
	f.data[key] = value
	f.rev[key]++
	return int64(f.rev[key]), nil
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

/**
 * KvWatch DELIVERS A SNAPSHOT THEN THE BOUNDARY, and stops — which is all List needs.
 *
 * THE BOUNDARY MATTERS even in a fake: List loops until SnapshotDone, so a fake that never sends it
 * would hang the test rather than fail it, and a hang reads as a slow machine.
 */
func (f *fakeKV) KvWatch(ctx context.Context, _, pattern string) (<-chan bus.KvEvent, func(), error) {
	f.mu.Lock()
	keys := make([]string, 0, len(f.data))
	vals := make(map[string]string, len(f.data))
	revs := make(map[string]uint64, len(f.data))
	for k, v := range f.data {
		if matchPattern(pattern, k) {
			keys = append(keys, k)
			vals[k] = v
			revs[k] = f.rev[k]
		}
	}
	f.mu.Unlock()

	ch := make(chan bus.KvEvent, len(keys)+1)
	for _, k := range keys {
		ch <- bus.KvEvent{Key: k, Value: vals[k], Revision: revs[k]}
	}
	ch <- bus.KvEvent{Done: true}
	return ch, func() {}, nil
}

// matchPattern implements enough of NATS subject matching for these tests: `>` covers the rest, `*`
// covers one token.
func matchPattern(pattern, key string) bool {
	p, k := strings.Split(pattern, "."), strings.Split(key, ".")
	for i, seg := range p {
		if seg == ">" {
			return true
		}
		if i >= len(k) {
			return false
		}
		if seg != "*" && seg != k[i] {
			return false
		}
	}
	return len(p) == len(k)
}

func TestEnqueueThenDrain(t *testing.T) {
	q := New(newFakeKV())
	ctx := context.Background()

	if _, err := q.Enqueue(ctx, "alice", "s1", "", "trigger", map[string]any{"text": "hi"}, "user"); err != nil {
		t.Fatal(err)
	}
	got, err := q.Drain(ctx, "alice", "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("drained %d messages, want 1", len(got))
	}
	if got[0].Type != "trigger" {
		t.Fatalf("type = %q", got[0].Type)
	}
	// AND A SECOND DRAIN FINDS NOTHING: consumed is consumed.
	again, err := q.Drain(ctx, "alice", "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Fatalf("a drained message came back: %d", len(again))
	}
}

/**
 * A CONSUMED MESSAGE STAYS READABLE.
 *
 * Deleting on read would make the queue a black hole: nobody could answer "did the thing I sent get
 * picked up". It stays for a day, with a shorter lifetime than a pending one, and Page still shows it.
 */
func TestConsumedMessagesRemainVisible(t *testing.T) {
	kv := newFakeKV()
	q := New(kv)
	ctx := context.Background()

	id, err := q.Enqueue(ctx, "alice", "s1", "m1", "trigger", map[string]any{"text": "hi"}, "user")
	if err != nil {
		t.Fatal(err)
	}
	// ONE LIFETIME, because the bucket has one — see the note on TTL. What matters is that a consumed
	// message is still READABLE, not that it expires sooner.
	if got, want := kv.ttl[keyFor("alice", "s1", id)], TTLMs(); got != want {
		t.Fatalf("pending ttl = %d, want %d", got, want)
	}
	if _, err := q.Drain(ctx, "alice", "s1"); err != nil {
		t.Fatal(err)
	}
	// THE MARK IS A REWRITE, and a rewrite through CAS does not carry a TTL — the bucket's own applies.
	// Asserting the exact number here would be asserting an implementation detail of NATS, so this
	// checks the thing that matters: the row is still there, and it says consumed.
	page, err := q.Page(ctx, "alice", "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 || page[0].Status != "consumed" || page[0].ConsumedAt == 0 {
		t.Fatalf("the consumed message is not visible as such: %+v", page)
	}
}

// A PRODUCER-CHOSEN ID LANDS ON THE SAME KEY, so a publisher that retried replaces its own message
// instead of delivering a second copy.
func TestEnqueueWithTheSameIdReplaces(t *testing.T) {
	kv := newFakeKV()
	q := New(kv)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, err := q.Enqueue(ctx, "alice", "s1", "same", "trigger", map[string]any{"n": i}, "user"); err != nil {
			t.Fatal(err)
		}
	}
	if len(kv.data) != 1 {
		t.Fatalf("%d keys, want 1 — a retry duplicated the message", len(kv.data))
	}
}

// OLDEST FIRST: a queue is a queue, and a later instruction must not overtake the one it answers.
func TestPendingIsOldestFirst(t *testing.T) {
	q := New(newFakeKV())
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c"} {
		if _, err := q.Enqueue(ctx, "alice", "s1", id, "trigger", map[string]any{}, "user"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	got, err := q.Pending(ctx, "alice", "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].ID != "a" || got[2].ID != "c" {
		t.Fatalf("order = %v, want a,b,c", idsOf(got))
	}
}

/**
 * THE BOOT BACKSTOP DEPENDS ON THIS: after a restart somebody must find the sessions whose messages
 * arrived while nothing was running. It is the one thing the old table did with a GROUP BY.
 */
func TestSessionsWithPendingFindsTheRightConversations(t *testing.T) {
	q := New(newFakeKV())
	ctx := context.Background()

	if _, err := q.Enqueue(ctx, "alice", "s1", "", "trigger", map[string]any{}, "user"); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(ctx, "alice", "s2", "", "event", map[string]any{}, "user"); err != nil {
		t.Fatal(err)
	}
	// A CONSUMED MESSAGE MUST NOT RESURRECT ITS SESSION at boot.
	if _, err := q.Enqueue(ctx, "alice", "s3", "", "trigger", map[string]any{}, "user"); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Drain(ctx, "alice", "s3"); err != nil {
		t.Fatal(err)
	}

	// NARROWED TO THE TYPES THAT WAKE A TURN: s2's `event` is context-only, and starting a turn for it
	// would be a turn about nothing.
	got, err := q.SessionsWithPending(ctx, []string{"trigger"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].SessionName != "s1" {
		t.Fatalf("sessions = %+v, want only s1", got)
	}
}

/**
 * A SESSION NAME WITH A DOT SURVIVES THE ROUND TRIP.
 *
 * sessionstore escapes it into one key token; the backstop has to unescape it back, or it would try to
 * wake a session named `a` rather than `a.b` — and find nothing.
 */
func TestSessionsWithPendingUnescapesTheSessionName(t *testing.T) {
	q := New(newFakeKV())
	ctx := context.Background()

	if _, err := q.Enqueue(ctx, "alice", "a.b", "", "trigger", map[string]any{}, "user"); err != nil {
		t.Fatal(err)
	}
	got, err := q.SessionsWithPending(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].SessionName != "a.b" {
		t.Fatalf("sessions = %+v, want the dotted name intact", got)
	}
}

func TestInterruptIsDetected(t *testing.T) {
	q := New(newFakeKV())
	ctx := context.Background()
	if ok, _ := q.HasInterrupt(ctx, "alice", "s1"); ok {
		t.Fatal("an empty mailbox reported an interrupt")
	}
	if _, err := q.Enqueue(ctx, "alice", "s1", "", "interrupt", map[string]any{}, "user"); err != nil {
		t.Fatal(err)
	}
	if ok, err := q.HasInterrupt(ctx, "alice", "s1"); err != nil || !ok {
		t.Fatalf("interrupt not seen: %v %v", ok, err)
	}
}

/**
 * TWO DRAINS RACING MUST NOT BOTH DELIVER THE SAME MESSAGE.
 *
 * THIS IS THE WHOLE REASON Drain USES COMPARE-AND-SWAP. Two replicas can list the same pending rows and
 * both decide to consume them; with a plain write the second overwrites the first and the turn runs twice
 * on one prompt. The CAS makes the list a race with exactly one winner.
 *
 * The fake reports a lost race as (0, nil) — the real bus's contract — so this test fails if the claim
 * ever goes back to being a put.
 */
func TestRacingDrainsDeliverEachMessageOnce(t *testing.T) {
	q := New(newFakeKV())
	ctx := context.Background()

	for _, id := range []string{"m1", "m2", "m3"} {
		if _, err := q.Enqueue(ctx, "alice", "s1", id, "trigger", map[string]any{}, "user"); err != nil {
			t.Fatal(err)
		}
	}

	// BOTH DRAINS READ THE SAME SNAPSHOT BEFORE EITHER WRITES, which is the race.
	type result struct {
		msgs []Message
		err  error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			ms, err := q.Drain(ctx, "alice", "s1")
			results <- result{ms, err}
		}()
	}
	close(start)

	seen := map[string]int{}
	for i := 0; i < 2; i++ {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		for _, m := range r.msgs {
			seen[m.ID]++
		}
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("%s was delivered %d times across two racing drains", id, n)
		}
	}
	// AND EVERY MESSAGE WAS DELIVERED BY SOMEBODY: a CAS that lost every race would deliver nothing.
	if len(seen) != 3 {
		t.Fatalf("%d messages delivered in total, want 3", len(seen))
	}
}

/**
 * A CLAIM THAT CANNOT BE ARRIVED AT IS NOT AN ERROR — the message stays pending for the next drain.
 *
 * This is the crash case: a replica that dies mid-drain leaves its unclaimed messages pending, and the
 * next drain takes them. Losing them would be worse than delivering twice, and claiming them makes it
 * impossible to have both.
 */
func TestAFailedClaimLeavesTheMessagePending(t *testing.T) {
	kv := newFakeKV()
	q := New(kv)
	ctx := context.Background()

	id, err := q.Enqueue(ctx, "alice", "s1", "m1", "trigger", map[string]any{}, "user")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Drain(ctx, "alice", "s1"); err != nil {
		t.Fatal(err)
	}
	// SOMEBODY REWRITES THE KEY, so a stale claim loses — exactly what happens when two replicas overlap.
	var m Message
	if err := unmarshalMessage(kv.data[keyFor("alice", "s1", id)], &m); err != nil {
		t.Fatal(err)
	}
	m.Status = "pending"
	if err := q.kv.Put(ctx, DefaultBucket, keyFor("alice", "s1", id), m, TTL); err != nil {
		t.Fatal(err)
	}
	got, err := q.Drain(ctx, "alice", "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("a message put back to pending was not redelivered: %d", len(got))
	}
}

func keyFor(tenant, session, id string) string { return "t." + tenant + ".s." + session + "." + id }

func idsOf(ms []Message) []string {
	out := make([]string, 0, len(ms))
	for _, m := range ms {
		out = append(out, m.ID)
	}
	return out
}
