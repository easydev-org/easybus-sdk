package job

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/easydev-org/easybus-sdk/bus"
)

/**
 * Registry is the shared list of jobs, held in a KV bucket.
 *
 * WHY THE BUS AND NOT A LOCAL MAP. A job outlives the tool call that started it, and the
 * two processes that care about it are different: the extension that runs it and the
 * agent that has to report its completion. A local map would be a private opinion each of
 * them holds; the bucket is the one list they both read.
 *
 * THE HEARTBEAT IS THE TTL. There is no separate liveness protocol: a running entry is
 * written with a short TTL and renewed while the work continues, so an extension that
 * stops renewing — because it crashed, was evicted, or was killed — stops being visible.
 * Liveness becomes a property of the store rather than a thing every participant has to
 * remember to check.
 */
type Registry struct {
	bus    bus.Bus
	bucket string

	// RunningTTL is how long a running entry survives without renewal.
	//
	// IT MUST EXCEED STALE_AFTER, and that is not a preference: the sweeper decides a job
	// is gone by finding a STILL-PRESENT entry whose renewal is old. If the TTL expired
	// first, the entry would vanish and there would be nothing left to judge — the job
	// would simply disappear instead of being recorded as lost.
	RunningTTL time.Duration
	// RenewEvery is how often a running entry is rewritten. A quarter of the TTL, so
	// three consecutive renewals can fail before the entry lapses.
	RenewEvery time.Duration
	// StaleAfter is how long without renewal means the owner is gone.
	StaleAfter time.Duration
	// DoneTTL is how long a finished entry stays readable.
	DoneTTL time.Duration
}

// Default timings, and the arithmetic behind each.
const (
	// 60s TTL, renewed every 15s: four renewals fit inside one lifetime, so a lost
	// renewal is not a lost job.
	defaultRunningTTL = 60 * time.Second
	defaultRenewEvery = 15 * time.Second
	// 30s stale: twice the renewal interval. One late renewal is tolerated, two
	// consecutive misses are not, and the entry is still present at this point —
	// which is exactly what the sweeper needs.
	defaultStaleAfter = 30 * time.Second
	// A day, because a finished job is history worth reading, not state worth holding.
	defaultDoneTTL = 24 * time.Hour
	// Exported bucket name so an operator can inspect it without reading this source.
	DefaultBucket = "jobs"
)

// NewRegistry returns a registry over the shared jobs bucket.
func NewRegistry(b bus.Bus) *Registry {
	return &Registry{
		bus:        b,
		bucket:     DefaultBucket,
		RunningTTL: defaultRunningTTL,
		RenewEvery: defaultRenewEvery,
		StaleAfter: defaultStaleAfter,
		DoneTTL:    defaultDoneTTL,
	}
}

/**
 * Key is the bucket key for one job.
 *
 * THE SEGMENTS ARE THERE TO BE WATCHED, not just to be unique. A client listing one
 * conversation asks for `t.<tenant>.s.<session>.*`, and a sweeper asks for every running
 * entry in a tenant; both are wildcard queries that only work if the parts are ordered
 * from least to most specific.
 */
func Key(tenant, session, jobID string) string {
	return fmt.Sprintf("t.%s.s.%s.%s", tenant, session, jobID)
}

/**
 * SessionPrefix is the wildcard for one conversation's jobs.
 *
 * IT ENDS IN `>`, NOT `*`, AND THAT IS THE WHOLE POINT. NATS subject wildcards are per TOKEN: `*`
 * matches exactly one, `>` matches one or more of the remaining. A job id contains dots —
 * `sandbox-job.392f82df` — so `t.<tenant>.s.<session>.*` matched the `sandbox-job` token and then
 * expected the subject to end, which it does not. The watch was live, the jobs were written, and the
 * list came back empty.
 */
func SessionPrefix(tenant, session string) string {
	return fmt.Sprintf("t.%s.s.%s.>", tenant, session)
}

// put writes an entry with a TTL.
func (r *Registry) put(ctx context.Context, e Entry, ttl time.Duration) error {
	body, err := marshalEntry(e)
	if err != nil {
		return err
	}
	_, err = r.bus.KVCreate(ctx, r.bucket, Key(e.Tenant, e.Session, e.ID), string(body), ttl.Milliseconds())
	if err != nil {
		// A key that already exists is not a create; fall through to a put so a
		// re-registration after a restart still lands.
		return r.bus.KVPut(ctx, r.bucket, Key(e.Tenant, e.Session, e.ID), string(body), ttl.Milliseconds())
	}
	return nil
}

/**
 * marshalEntry and unmarshalEntry are the ONLY places an entry touches JSON.
 *
 * ONE ENCODER AND ONE DECODER, so the writer and every reader — the watcher, the sweeper,
 * the client — cannot drift on field names. A second json.Marshal somewhere else is how a
 * producer ends up emitting `exitCode` while a consumer reads `exit_code`.
 */
func marshalEntry(e Entry) ([]byte, error) { return json.Marshal(e) }

func unmarshalEntry(raw string, e *Entry) error { return json.Unmarshal([]byte(raw), e) }

/**
 * Start records a job as running and returns the handle that renews it.
 *
 * THE CALLER MUST CLOSE THE HANDLE. Until it is closed the entry is renewed, and a job
 * the extension has forgotten about would otherwise stay visible for as long as the
 * process lives — the opposite of what a heartbeat is for.
 */
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
	return Handle{reg: r, entry: e, done: make(chan struct{})}, nil
}

// Handle renews one running entry until it is finished or abandoned.
type Handle struct {
	reg   *Registry
	entry Entry
	done  chan struct{}
	once  bool
}

// KeepAlive renews the entry every RenewEvery until ctx ends or Finish is called.
//
// RUN IT IN A GOROUTINE. It blocks, and it is meant to: the reason to have a heartbeat is
// that somebody is still there, and that is only true while the work is being done.
func (h *Handle) KeepAlive(ctx context.Context) {
	if h.reg == nil {
		return
	}
	t := time.NewTicker(h.reg.RenewEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.done:
			return
		case <-t.C:
			h.entry.RenewedAt = time.Now().UnixMilli()
			if err := h.reg.put(ctx, h.entry, h.reg.RunningTTL); err != nil {
				// A FAILED RENEWAL IS NOT AN ERROR TO ACT ON. The entry will lapse, and
				// the sweeper will record it as lost — which is the correct outcome for
				// an extension that cannot reach the bus. Logged, not fatal.
				slog.WarnContext(ctx, "job: renew failed", "svc", "job", "job", h.entry.ID, "err", err)
			}
		}
	}
}

/**
 * Finish records a terminal state and stops renewing.
 *
 * THE TTL CHANGES HERE, from the short running one to the long finished one. That is the
 * whole reason the two exist: a running entry that stops being renewed is a corpse, and
 * should become visible as one quickly; a finished entry is a record, and should stay
 * readable for as long as anybody might look.
 */
func (h *Handle) Finish(ctx context.Context, state State, exitCode *int, reason string) error {
	if h.reg == nil {
		return nil
	}
	h.stop()
	e := h.entry
	e.State = state
	e.ExitCode = exitCode
	e.Reason = reason
	e.FinishedAt = time.Now().UnixMilli()
	e.RenewedAt = e.FinishedAt
	return h.reg.put(ctx, e, h.reg.DoneTTL)
}

// stop closes the renew loop exactly once.
func (h *Handle) stop() {
	if h.once {
		return
	}
	h.once = true
	close(h.done)
}

// Get reads one entry.
func (r *Registry) Get(ctx context.Context, tenant, session, jobID string) (Entry, bool, error) {
	raw, err := r.bus.KVGet(ctx, r.bucket, Key(tenant, session, jobID))
	if err != nil || raw == "" {
		return Entry{}, false, nil
	}
	var e Entry
	if err := unmarshalEntry(raw, &e); err != nil {
		return Entry{}, false, err
	}
	return e, true, nil
}

/**
 * Put rewrites an entry, choosing the TTL from its state.
 *
 * IT EXISTS FOR CANCELLATION, which is the one thing that changes an entry from OUTSIDE the handle
 * that started it. The handle owns renewal and finishing; a cancel arrives on a different code path
 * and has no handle to call Finish on.
 *
 * THE TTL FOLLOWS THE STATE: a rewrite that leaves the job running gets the short one and keeps
 * needing renewal, and one that records an ending gets the long one. Getting this wrong in the
 * direction of "always long" would leave cancelled jobs renewed forever by nobody.
 */
func (r *Registry) Put(ctx context.Context, e Entry) error {
	ttl := r.RunningTTL
	if e.State.Terminal() {
		ttl = r.DoneTTL
	}
	return r.put(ctx, e, ttl)
}

/**
 * Watch streams one conversation's jobs: the current list first, then changes.
 *
 * THE INITIAL SNAPSHOT IS WHAT MAKES A PANEL OPEN INSTANTLY. A watch that only delivered
 * changes would show an empty list until something moved, which reads as "no jobs" when
 * the truth is "several, all running steadily".
 *
 * THE SNAPSHOT BOUNDARY IS KEPT, not thrown away. The bus marks the end of the retained
 * snapshot with one synthetic event; a client needs it to know when "here is what exists"
 * becomes "here is what changed", because only before that point is an ABSENT key evidence
 * of absence. Without the marker a client either renders a half-filled list or has to
 * guess how long to wait.
 */
func (r *Registry) Watch(ctx context.Context, tenant, session string) (<-chan KvEvent, func(), error) {
	ch, cancel, err := r.bus.KvWatch(ctx, r.bucket, SessionPrefix(tenant, session))
	if err != nil {
		return nil, nil, err
	}
	out := make(chan KvEvent)
	go func() {
		defer close(out)
		for ev := range ch {
			mine := KvEvent{Deleted: ev.Deleted, SnapshotDone: ev.Done}
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
	/**
	 * SnapshotDone marks the boundary between "what already exists" and "what changed".
	 *
	 * IT ARRIVES ONCE, and it arrives LAST of the snapshot. Everything after it is live.
	 */
	SnapshotDone bool
}
