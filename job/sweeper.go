package job

import (
	"context"
	"log/slog"
	"time"
)

/**
 * Sweeper records the jobs whose owner stopped renewing.
 *
 * NOBODY ELSE CAN WRITE THIS. An extension cannot report its own death — if it could still
 * write, it would still be renewing — so the judgement has to be made by a third party
 * that is still alive and can still see the entry.
 *
 * IT JUDGES STALENESS, NOT ABSENCE, AND THE DISTINCTION IS THE DESIGN. A missing key says
 * only that nothing is there now; it does not say what was. A key that is PRESENT with an
 * old RenewedAt says exactly what this needs: something claimed to be running and then
 * stopped saying so. That is why the running TTL is longer than the staleness threshold —
 * the entry must still exist when the judgement is made.
 */
type Sweeper struct {
	reg *Registry
	// Interval is how often the bucket is examined. It does not need to be quick: a job
	// that has been stalled for a minute is not made worse by waiting another five
	// seconds to be recorded.
	Interval time.Duration
}

// NewSweeper returns a sweeper over a registry.
func NewSweeper(reg *Registry) *Sweeper {
	return &Sweeper{reg: reg, Interval: 5 * time.Second}
}

/**
 * Run sweeps until ctx ends.
 *
 * IT IS SAFE TO RUN SEVERAL. The write is a compare-and-set on the revision that was
 * read, so two sweepers that see the same stale entry cannot both record it — the second
 * one's write fails and it moves on. That matters because a second replica is exactly
 * what somebody will add when they are worried about the first one being down.
 */
func (s *Sweeper) Run(ctx context.Context) {
	if s.reg == nil {
		return
	}
	t := time.NewTicker(s.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.sweep(ctx)
		}
	}
}

// sweep examines the bucket once.
func (s *Sweeper) sweep(ctx context.Context) {
	// THE WATCH GIVES THE CURRENT STATE, which is why a sweep is a short-lived watch
	// rather than a separate list operation: the bucket already has one way to read
	// everything, and inventing a second would mean two places to keep correct.
	/**
	 * `>` AND NOT `*`. A job key has five tokens — `t.<tenant>.s.<session>.<id>` — so `*` matched the
	 * first token only and the watch saw NOTHING. The sweeper was running, on a bucket it could not
	 * read, and a job whose owner died was never recorded as lost: it simply vanished when its TTL
	 * lapsed, which is the one outcome this whole mechanism exists to prevent.
	 */
	ch, cancel, err := s.reg.kv.Watch(ctx, s.reg.bucket, ">")
	if err != nil {
		slog.WarnContext(ctx, "job: sweep cannot read the bucket", "svc", "job", "err", err)
		return
	}
	defer cancel()

	now := time.Now().UnixMilli()
	staleAfter := s.reg.StaleAfter.Milliseconds()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			// THE SNAPSHOT IS THE WHOLE STATE; once it ends, this sweep is done. Live
			// events describe changes that are not this sweep's business — the next
			// tick will see them.
			if ev.SnapshotDone {
				return
			}
			if ev.Deleted || ev.Value == "" {
				continue
			}
			var e Entry
			if err := unmarshalEntry(ev.Value, &e); err != nil {
				continue
			}
			if e.State != StateRunning {
				continue
			}
			if now-e.RenewedAt <= staleAfter {
				continue
			}
			s.markLost(ctx, e, int64(ev.Revision))
		}
	}
}

/**
 * markLost rewrites one stalled entry as lost.
 *
 * THE REVISION GUARD IS WHAT MAKES THIS SAFE. Between reading the entry and writing it,
 * the owner may have renewed — in which case the write must not happen, because the job
 * is alive and calling it lost would report a death that did not occur. Compare-and-set
 * turns that race into a failed write rather than a wrong state.
 */
//
// THE REVISION IS AN int64 HERE because that is what KVCas takes, while the watch reports
// it as uint64. The conversion is safe in one direction only, and this is that direction:
// a revision is a counter that starts at 1 and grows, never a value that needs the top bit.
func (s *Sweeper) markLost(ctx context.Context, e Entry, revision int64) {
	e.State = StateLost
	e.Reason = "the extension running this job stopped reporting"
	e.FinishedAt = time.Now().UnixMilli()
	body, err := marshalEntry(e)
	if err != nil {
		return
	}
	if _, err := s.reg.bus.KVCas(ctx, s.reg.bucket, Key(e.Tenant, e.Session, e.ID), string(body), revision); err != nil {
		// EITHER THE OWNER RENEWED (the good case) or another sweeper got there first
		// (also fine). Neither is worth a warning: the outcome is correct either way.
		slog.DebugContext(ctx, "job: lost-mark skipped, entry moved", "svc", "job", "job", e.ID)
		return
	}
	slog.InfoContext(ctx, "job: marked lost", "svc", "job", "job", e.ID, "ext", e.ExtID)
}
