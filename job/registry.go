package job

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/sessionstore"
)

// Registry is the shared list of jobs, held in a KV bucket.
//
// WHY THE BUS AND NOT A LOCAL MAP. A job outlives the tool call that started it, and the
// two processes that care about it are different: the extension that runs it and the
// agent that has to report its completion. A local map would be a private opinion each of
// them holds; the bucket is the one list they both read.
//
// THE HEARTBEAT IS THE TTL. There is no separate liveness protocol: a running entry is
// written with a short TTL and renewed while the work continues, so an extension that
// stops renewing — because it crashed, was evicted, or was killed — stops being visible.
// Liveness becomes a property of the store rather than a thing every participant has to
// remember to check.
type Registry struct {
	kv *sessionstore.Store
	// bus is kept for the ONE operation sessionstore does not offer: a compare-and-set. CAS is not
	// part of "read and write a list" — it is how the sweeper avoids overwriting a renewal that landed
	// between its read and its write — so it stays here rather than becoming a shape every list has
	// to think about.
	bus    bus.Bus
	bucket string

	// RunningTTL is how long a running entry survives without renewal.
	//
	// IT MUST EXCEED STALE_AFTER, and that is not a preference: the sweeper decides a job
	// is gone by finding a STILL-PRESENT entry whose renewal is old. If the TTL expired
	// first, the entry would vanish and there would be nothing left to judge — the job
	// would simply disappear instead of being recorded as lost.
	//
	// IT IS NOT THE RENEWAL WINDOW, WHICH IS A COMMON READING AND THE WRONG ONE. Liveness is
	// `RenewedAt` against StaleAfter; the TTL only decides when the STORE forgets the key. So this
	// being the same as DoneTTL costs nothing but a few stale rows, while making it short would
	// DELETE EVIDENCE — and NATS KV makes the choice for us anyway (see below).
	RunningTTL time.Duration
	// RenewEvery is how often a running entry is rewritten. A quarter of the TTL, so
	// three consecutive renewals can fail before the entry lapses.
	RenewEvery time.Duration
	// StaleAfter is how long without renewal means the owner is gone.
	StaleAfter time.Duration
	// DoneTTL is how long a finished entry stays readable.
	DoneTTL time.Duration

	// live maps a bucket key to the handle renewing it IN THIS PROCESS.
	//
	// IT EXISTS SO A CANCELLATION CAN STOP THE HEARTBEAT EXACTLY, not eventually. `Put` arrives
	// on a different code path than the handle that owns the job, so without this map the
	// heartbeat would keep writing `running` over the cancellation until it noticed on its next
	// tick — and if nobody ever noticed, forever.
	mu   sync.Mutex
	live map[string]Handle

	// writeMu serialises RENEWAL against a TERMINAL WRITE.
	//
	// Both happen in this process — cancellation reaches `Put` through a tool handler, and the
	// heartbeat is a goroutine here — so a mutex makes them exact rather than eventually correct.
	// `stopLiveHandle` closes the done channel, but a renewal that is already past its select will
	// still write once afterwards; that write lands between the cancel and the reader's next look,
	// and it says `running`.
	//
	// Holding this across the check and the write means a terminal write cannot be overtaken: the
	// renewal either finishes first (and is then overwritten, correctly), or waits and sees `done`.
	writeMu sync.Mutex
}

// Default timings, and the arithmetic behind each.
//
// ONE TTL, NOT TWO, BECAUSE NATS KV HAS ONE. Its TTL is a BUCKET setting — `CreateKeyValue(TTL: …)`,
// applied once when the bucket is created — and `Update` resets it to that same bucket value. A per-key
// lifetime cannot be expressed, so the two constants below are equal on purpose rather than by
// coincidence: a running entry and a finished one both live a day, and what distinguishes them is
// `State` and `RenewedAt`.
//
// THIS IS NOT A COMPROMISE, ONCE THE ROLES ARE SEPARATED. The TTL is the store's forgetting policy; the
// heartbeat is the liveness signal. Writing the entry for sixty seconds and relying on expiry to mean
// "the extension died" was asking the store to carry a judgement it cannot make — and it would have
// deleted the entry before the sweeper could record it as lost, which is the one outcome that mechanism
// exists to prevent.
const (
	// A day, matching DoneTTL — see the note above.
	defaultRunningTTL = 24 * time.Hour
	defaultRenewEvery = 15 * time.Second
	// 30s stale: twice the renewal interval. One late renewal is tolerated, two
	// consecutive misses are not. The entry is certainly still present at this point, which is what the
	// sweeper needs — and is the reason RunningTTL must stay comfortably above this.
	defaultStaleAfter = 30 * time.Second
	// A day, because a finished job is history worth reading, not state worth holding.
	defaultDoneTTL = 24 * time.Hour
	// Exported bucket name so an operator can inspect it without reading this source.
	DefaultBucket = "jobs"
)

// NewRegistry returns a registry over the shared jobs bucket.
func NewRegistry(b bus.Bus) *Registry {
	return &Registry{
		kv:         sessionstore.New(b),
		bus:        b,
		bucket:     DefaultBucket,
		RunningTTL: defaultRunningTTL,
		RenewEvery: defaultRenewEvery,
		StaleAfter: defaultStaleAfter,
		DoneTTL:    defaultDoneTTL,
	}
}

// Key is the bucket key for one job. The shape and its reasons live in sessionstore, which every
// per-session list in this SDK shares.
func Key(tenant, session, jobID string) string {
	return sessionstore.Key(tenant, session, jobID)
}

// SessionPrefix is the wildcard for one conversation's jobs.
func SessionPrefix(tenant, session string) string {
	return sessionstore.Prefix(tenant, session)
}

// put writes an entry with the given TTL. The "put, never create" rule lives in sessionstore.
func (r *Registry) put(ctx context.Context, e Entry, ttl time.Duration) error {
	return r.kv.Put(ctx, r.bucket, Key(e.Tenant, e.Session, e.ID), e, ttl)
}

// unmarshalEntry is the ONLY place a job becomes bytes or back.
//
// ONE DECODER, so the writer and every reader — the watcher, the sweeper, the client — cannot drift on
// field names. A second decoder somewhere else is how a producer ends up emitting `exitCode` while a
// consumer reads `exit_code`.
func unmarshalEntry(raw string, e *Entry) error { return json.Unmarshal([]byte(raw), e) }

// marshalEntry exists for the sweeper's compare-and-set, which needs the bytes to hand to KVCas
// rather than a value to write through the store.
func marshalEntry(e Entry) ([]byte, error) { return json.Marshal(e) }

// Start records a job as running and returns the handle that renews it.
//
// THE CALLER MUST CLOSE THE HANDLE. Until it is closed the entry is renewed, and a job
// the extension has forgotten about would otherwise stay visible for as long as the
// process lives — the opposite of what a heartbeat is for.
func (r *Registry) Start(ctx context.Context, e Entry) (Handle, error) {
	now := time.Now().UnixMilli()
	e.State = StateRunning
	e.StartedAt = now
	if e.RenewedAt == 0 {
		e.RenewedAt = now
	}
	if err := r.put(ctx, e, r.RunningTTL); err != nil {
		return Handle{}, err
	}
	h := Handle{state: &handleState{reg: r, entry: e, done: make(chan struct{}), key: Key(e.Tenant, e.Session, e.ID)}}
	r.track(h)
	return h, nil
}

// Handle renews one running entry until it is finished or abandoned.
//
// IT IS A POINTER TO SHARED STATE, not a value holding it. `Start` returns this handle by value and
// also registers it in `Registry.live` so a cancellation can stop it; if the fields lived in the
// struct, the caller's copy and the registry's copy would each have their own `stopOnce` while
// sharing one channel — and stopping through both paths closes that channel twice.
type Handle struct {
	state *handleState
}

type handleState struct {
	reg   *Registry
	entry Entry
	// key is the bucket key, kept so Registry can find this handle from a Put that
	// arrived on a different code path (cancellation) and stop it.
	key string
	// stopOnce guards `done` against a second close. Finish stops the loop and then writes
	// through Put, which stops it again on the way.
	stopOnce sync.Once
	done     chan struct{}
}

// track registers a live handle so an external terminal write can stop it.
func (r *Registry) track(h Handle) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.live == nil {
		r.live = make(map[string]Handle)
	}
	r.live[h.state.key] = h
}

// untrack forgets a handle. Called by stop, so a finished job cannot be reached again.
func (r *Registry) untrack(h Handle) {
	if h.state.key == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur, ok := r.live[h.state.key]; ok && cur.state == h.state {
		delete(r.live, h.state.key)
	}
}

// stopLiveHandle stops the handle renewing the given key, if this process owns one.
//
// THE CROSS-PROCESS CASE IS NOT THIS. A cancel for a job this process did not start has no
// handle here to stop, and the state check in KeepAlive covers it — see the note there.
func (r *Registry) stopLiveHandle(key string) {
	r.mu.Lock()
	h, ok := r.live[key]
	r.mu.Unlock()
	if ok {
		h.stop()
	}
}

// KeepAlive renews the entry every RenewEvery until ctx ends or Finish is called.
//
// RUN IT IN A GOROUTINE. It blocks, and it is meant to: the reason to have a heartbeat is
// that somebody is still there, and that is only true while the work is being done.
func (h *Handle) KeepAlive(ctx context.Context) {
	if h.state == nil || h.state.reg == nil {
		return
	}
	t := time.NewTicker(h.state.reg.RenewEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.state.done:
			return
		case <-t.C:
			// RE-READ BEFORE WRITING, BECAUSE A RENEWAL CAN UNDO A CANCELLATION.
			//
			// `entry` is a copy taken at Start, so writing it back unconditionally restores
			// `state: running` over whatever somebody else recorded — every interval, forever. A
			// cancelled job then stays visible as running and keeps being renewed, which is worse
			// than not having a cancel at all.
			//
			// TWO MECHANISMS, and both are needed:
			//
			//   * `stopLiveHandle` closes the done channel before a cancel writes, which ends this
			//     loop — but only for a job THIS PROCESS owns.
			//   * this check covers the rest: a cancel from another replica, or an entry a sweeper
			//     marked lost.
			//
			// writeMu is what makes the in-process case exact. Closing `done` does not stop a
			// renewal that is already past its select; without the mutex that write could land
			// after the cancel and restore `running` for one interval.
			h.state.reg.writeMu.Lock()
			select {
			case <-h.state.done:
				h.state.reg.writeMu.Unlock()
				return
			default:
			}
			cur, found, err := h.state.reg.Get(ctx, h.state.entry.Tenant, h.state.entry.Session, h.state.entry.ID)
			if err != nil {
				h.state.reg.writeMu.Unlock()
				// A FAILED READ IS NOT A FAILED RENEWAL, so it does not stop the heartbeat: the bus
				// being briefly unreachable says nothing about whether this job is alive.
				slog.WarnContext(ctx, "job: renew could not read the entry", "svc", "job", "job", h.state.entry.ID, "err", err)
				continue
			}
			if found && (cur.State != StateRunning || cur.ExtID != h.state.entry.ExtID) {
				h.state.reg.writeMu.Unlock()
				// Somebody else has decided this job's fate. Do not argue.
				h.stop()
				return
			}
			h.state.entry.RenewedAt = time.Now().UnixMilli()
			err = h.state.reg.put(ctx, h.state.entry, h.state.reg.RunningTTL)
			h.state.reg.writeMu.Unlock()
			if err != nil {
				// A FAILED RENEWAL IS NOT AN ERROR TO ACT ON. The entry will lapse, and
				// the sweeper will record it as lost — which is the correct outcome for
				// an extension that cannot reach the bus. Logged, not fatal.
				slog.WarnContext(ctx, "job: renew failed", "svc", "job", "job", h.state.entry.ID, "err", err)
			}
		}
	}
}

// Finish records a terminal state and stops renewing.
//
// THE TTL CHANGES HERE, from the short running one to the long finished one. That is the
// whole reason the two exist: a running entry that stops being renewed is a corpse, and
// should become visible as one quickly; a finished entry is a record, and should stay
// readable for as long as anybody might look.
func (h *Handle) Finish(ctx context.Context, state State, exitCode *int, reason string) error {
	if h.state == nil || h.state.reg == nil {
		return nil
	}
	h.stop()
	e := h.state.entry
	e.State = state
	e.ExitCode = exitCode
	e.Reason = reason
	e.FinishedAt = time.Now().UnixMilli()
	e.RenewedAt = e.FinishedAt
	if state.Terminal() {
		// A FINISHED JOB IS NOT CANCELLABLE. `Cancelable` describes whether a cancel could do
		// anything, and after the ending the answer is no — leaving the mode set made every
		// reader re-derive that from the state, and one that forgot drew a button that did nothing.
		e.CancelMode = CancelNone
	}
	return h.state.reg.Put(ctx, e)
}

// stop closes the renew loop exactly once, and stops the registry from finding this handle.
func (h *Handle) stop() {
	h.state.stopOnce.Do(func() {
		close(h.state.done)
		if h.state.reg != nil {
			h.state.reg.untrack(*h)
		}
	})
}

// Get reads one entry.
func (r *Registry) Get(ctx context.Context, tenant, session, jobID string) (Entry, bool, error) {
	var e Entry
	ok, err := r.kv.Get(ctx, r.bucket, Key(tenant, session, jobID), &e)
	if err != nil {
		return Entry{}, false, err
	}
	return e, ok, nil
}

// Put rewrites an entry, choosing the TTL from its state.
//
// IT EXISTS FOR CANCELLATION, which is the one thing that changes an entry from OUTSIDE the handle
// that started it. The handle owns renewal and finishing; a cancel arrives on a different code path
// and has no handle to call Finish on.
//
// A TERMINAL WRITE ALSO STOPS THE OWNING HANDLE. Without that the heartbeat keeps its own copy of
// the entry and writes `running` back over the cancellation on its next tick — the cancel is recorded
// and then silently undone, which is worse than refusing it. The handle is found by key, so this
// stops renewal for a job this process owns whether or not the caller knew it did.
//
// THE TTL FOLLOWS THE STATE: a rewrite that leaves the job running gets the short one and keeps
// needing renewal, and one that records an ending gets the long one. Getting this wrong in the
// direction of "always long" would leave cancelled jobs renewed forever by nobody.
func (r *Registry) Put(ctx context.Context, e Entry) error {
	ttl := r.RunningTTL
	if e.State.Terminal() {
		ttl = r.DoneTTL
		// THE SAME LOCK AS THE HEARTBEAT, so the pair is atomic: either a renewal finishes first
		// and this write replaces it, or this write lands and the renewal sees `done` and stops.
		r.writeMu.Lock()
		defer r.writeMu.Unlock()
		// STOP BEFORE WRITING. A renewal landing between the write and the stop would restore
		// `running` over the ending we just recorded.
		r.stopLiveHandle(Key(e.Tenant, e.Session, e.ID))
	}
	return r.put(ctx, e, ttl)
}

// Watch streams one conversation's jobs: the current list first, then changes.
//
// THE INITIAL SNAPSHOT IS WHAT MAKES A PANEL OPEN INSTANTLY. A watch that only delivered
// changes would show an empty list until something moved, which reads as "no jobs" when
// the truth is "several, all running steadily".
//
// THE SNAPSHOT BOUNDARY IS KEPT, not thrown away. The bus marks the end of the retained
// snapshot with one synthetic event; a client needs it to know when "here is what exists"
// becomes "here is what changed", because only before that point is an ABSENT key evidence
// of absence. Without the marker a client either renders a half-filled list or has to
// guess how long to wait.
func (r *Registry) Watch(ctx context.Context, tenant, session string) (<-chan KvEvent, func(), error) {
	ch, cancel, err := r.kv.Watch(ctx, r.bucket, SessionPrefix(tenant, session))
	if err != nil {
		return nil, nil, err
	}
	out := make(chan KvEvent)
	go func() {
		defer close(out)
		for ev := range ch {
			mine := KvEvent{Deleted: ev.Deleted, SnapshotDone: ev.SnapshotDone}
			// A KEY WE CANNOT PARSE IS SKIPPED, not fatal: another version of this code
			// may have written the entry, and taking the whole watch down would hide
			// every other job in the list because of one row it did not recognise.
			if !ev.Deleted && ev.Value != "" {
				var e Entry
				if unmarshalEntry(ev.Value, &e) == nil {
					mine.Job = e
					mine.HasValue = true
				}
			}
			select {
			case <-ctx.Done():
				return
			case out <- mine:
			}
		}
	}()
	return out, cancel, nil
}

// KvEvent is one change to the job list.
type KvEvent struct {
	// Job is the entry, decoded. Only meaningful when HasValue is true.
	Job Entry
	// HasValue is false for a deletion, and for an entry this version cannot decode.
	HasValue bool
	// Deleted means the key is gone — either it aged out, or its owner stopped renewing.
	Deleted bool
	// SnapshotDone marks the boundary between "what already exists" and "what changed".
	//
	// IT ARRIVES ONCE, and it arrives LAST of the snapshot. Everything after it is live.
	SnapshotDone bool
}
