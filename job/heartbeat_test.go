package job

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/easydev-org/easybus-sdk/bus"
)

/**
 * A CANCELLATION MUST NOT BE UNDONE BY THE HEARTBEAT.
 *
 * `Put` is how cancellation reaches the list, and it runs on a different code path than the handle
 * that renews the job. The handle holds a COPY of the entry taken at Start, so a renewal that writes
 * it back unconditionally restores `state: running` over the cancellation — every interval, forever.
 *
 * THE SYMPTOM IS THE WORST KIND: the cancel call succeeds, the panel shows the job stopped, and a
 * minute later it is running again because nobody was watching the list in between. A user concludes
 * the machine ignored them.
 */

// memKV is a KV that remembers one key and its revision.
type memKV struct {
	bus.Bus
	mu  sync.Mutex
	val string
	rev int64
	// puts counts writes, so "did the heartbeat stop" is a number rather than a guess.
	puts int
}

func (m *memKV) KVPut(_ context.Context, _, _, value string, _ int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.val = value
	m.rev++
	m.puts++
	return nil
}

func (m *memKV) KVGet(_ context.Context, _, _ string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.val, nil
}

func (m *memKV) KVCas(_ context.Context, _, _, value string, revision int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if revision != m.rev {
		return 0, fmt.Errorf("revision conflict")
	}
	m.val = value
	m.rev++
	return m.rev, nil
}

func (m *memKV) puts_() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.puts
}

func TestCancelSurvivesTheHeartbeat(t *testing.T) {
	kv := &memKV{}
	r := NewRegistry(kv)
	r.RenewEvery = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, err := r.Start(ctx, Entry{ID: "sandbox-job.j1", Kind: "sandbox-job", Tenant: "t", Session: "s"})
	if err != nil {
		t.Fatal(err)
	}
	go h.KeepAlive(ctx)

	// Let a few renewals land, so the handle is definitely alive and writing.
	time.Sleep(40 * time.Millisecond)

	e, found, err := r.Get(ctx, "t", "s", "sandbox-job.j1")
	if err != nil || !found {
		t.Fatalf("entry missing before cancel: found=%v err=%v", found, err)
	}
	e.State = StateCancelled
	e.FinishedAt = time.Now().UnixMilli()
	if err := r.Put(ctx, e); err != nil {
		t.Fatal(err)
	}

	// The window in which the old code overwrote it: several renewal intervals.
	writesAtCancel := kv.puts_()
	time.Sleep(80 * time.Millisecond)

	after, _, _ := r.Get(ctx, "t", "s", "sandbox-job.j1")
	if after.State != StateCancelled {
		t.Fatalf("the heartbeat undid the cancellation: state is %q after %d writes",
			after.State, kv.puts_()-writesAtCancel)
	}
	// AND IT STOPPED WRITING. A handle that is merely re-reading and not renewing would also pass
	// the check above, while still costing a read every interval for a job that is over.
	if n := kv.puts_() - writesAtCancel; n > 1 {
		t.Fatalf("%d writes after the cancel; the heartbeat did not stop", n)
	}
}

// A finished job must not advertise that it can be cancelled.
func TestFinishClearsTheCancelMode(t *testing.T) {
	kv := &memKV{}
	r := NewRegistry(kv)
	ctx := context.Background()

	h, err := r.Start(ctx, Entry{
		ID: "j", Kind: "sandbox-job", Tenant: "t", Session: "s", CancelMode: CancelKill,
	})
	if err != nil {
		t.Fatal(err)
	}
	code := 0
	if err := h.Finish(ctx, StateDone, &code, ""); err != nil {
		t.Fatal(err)
	}
	got, found, err := r.Get(ctx, "t", "s", "j")
	if err != nil || !found {
		t.Fatalf("entry missing: found=%v err=%v", found, err)
	}
	if got.CancelMode != CancelNone {
		t.Fatalf("a finished job still says it can be cancelled: %q", got.CancelMode)
	}
	if got.CancelMode.Cancelable() {
		t.Fatal("Cancelable() is true for a finished job")
	}
}

// A job that someone else recorded as lost must stop being renewed.
func TestHeartbeatYieldsWhenSweptToLost(t *testing.T) {
	kv := &memKV{}
	r := NewRegistry(kv)
	r.RenewEvery = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, err := r.Start(ctx, Entry{ID: "j2", Kind: "sandbox-job", Tenant: "t", Session: "s"})
	if err != nil {
		t.Fatal(err)
	}
	go h.KeepAlive(ctx)
	time.Sleep(30 * time.Millisecond)

	// The sweeper's judgement, written from elsewhere.
	e, _, _ := r.Get(ctx, "t", "s", "j2")
	e.State = StateLost
	e.Reason = "the extension running this job stopped reporting"
	if err := r.Put(ctx, e); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)

	after, _, _ := r.Get(ctx, "t", "s", "j2")
	if after.State != StateLost {
		t.Fatalf("a lost job was renewed back to %q", after.State)
	}
}
