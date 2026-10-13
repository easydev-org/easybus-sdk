package agent

import (
	"context"
	"time"

	"github.com/easydev-org/easybus-sdk/protocol"
)

// Session lease semantics (protocol-level, cross-replica):
//
//   - claim:  kvCreate on the lease bucket — succeeds only when no live
//     lease exists; returns the KV revision needed to renew.
//   - renew:  CAS on that revision; returns the NEW revision or null when
//     the lease was lost (expired or taken over after expiry).
//   - release: delete the key (back to idle).
//   - isRunning: key present.
//
// Every replica must use the same bucket and TTL for the mutual exclusion
// to hold — the bucket name and default TTL are protocol constants.

const (
	// LeaseBucket holds one key per running session
	// (easybus-session-state/<sessionToken> = "running").
	LeaseBucket = "easybus-session-state"
	// LeaseTTLDefaultMs is the default claim/renew TTL (30s).
	LeaseTTLDefaultMs = int64(30_000)
)

// ClaimSession atomically claims the session's run lease. Returns the KV
// revision on success (feed it to RenewSession), or (0, false) when another
// holder owns it.
func (a *Agent) ClaimSession(ctx context.Context, tenant, sessionName string, ttlMs int64) (int64, bool) {
	if ttlMs <= 0 {
		ttlMs = LeaseTTLDefaultMs
	}
	rev, err := a.b.KVCreate(ctx, LeaseBucket, protocol.TenantKVKey(tenant, protocol.SessionToken(sessionName)), "running", ttlMs)
	if err != nil || rev == 0 {
		return 0, false
	}
	return rev, true
}

// RenewSession extends the lease via CAS. Returns the new revision, or
// (0, false) when the lease was lost (expired and possibly re-claimed).
func (a *Agent) RenewSession(ctx context.Context, tenant, sessionName string, revision int64, ttlMs int64) (int64, bool) {
	if ttlMs <= 0 {
		ttlMs = LeaseTTLDefaultMs
	}
	rev, err := a.b.KVCas(ctx, LeaseBucket, protocol.TenantKVKey(tenant, protocol.SessionToken(sessionName)), "running", revision)
	if err != nil || rev == 0 {
		return 0, false
	}
	return rev, true
}

// ReleaseSession releases the run lease (back to idle).
func (a *Agent) ReleaseSession(ctx context.Context, tenant, sessionName string) error {
	return a.b.KVDelete(ctx, LeaseBucket, protocol.TenantKVKey(tenant, protocol.SessionToken(sessionName)))
}

// IsSessionRunning reports whether the session's run lease is held.
func (a *Agent) IsSessionRunning(ctx context.Context, tenant, sessionName string) bool {
	v, err := a.b.KVGet(ctx, LeaseBucket, protocol.TenantKVKey(tenant, protocol.SessionToken(sessionName)))
	return err == nil && v != ""
}

// WithSessionLease runs fn while holding the session's exclusive cross-replica
// lease. Returns (false, nil) when another replica already holds it. The
// lease is renewed on a timer (TTL/3) and released after fn returns; ctx is
// cancelled if the lease is lost mid-run (fn must observe ctx).
func (a *Agent) WithSessionLease(ctx context.Context, tenant, sessionName string, fn func(ctx context.Context) error, ttlMs int64) (bool, error) {
	if ttlMs <= 0 {
		ttlMs = LeaseTTLDefaultMs
	}
	revision, ok := a.ClaimSession(ctx, tenant, sessionName, ttlMs)
	if !ok {
		return false, nil
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	lost := make(chan struct{})
	go func() {
		// Renew on TTL/3 so a long-running turn survives single-renew hiccups.
		t := time.NewTicker(time.Duration(ttlMs/3) * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-t.C:
				next, ok := a.RenewSession(ctx, tenant, sessionName, revision, ttlMs)
				if !ok {
					close(lost)
					cancel()
					return
				}
				revision = next
			}
		}
	}()

	err := fn(runCtx)
	_ = a.ReleaseSession(context.WithoutCancel(ctx), tenant, sessionName)
	// If the lease was lost mid-run, surface it through the error channel.
	if err == nil {
		select {
		case <-lost:
			return true, context.Canceled
		default:
		}
	}
	return true, err
}
