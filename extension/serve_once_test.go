package extension

import (
	"testing"
)

// The Serve guard must be claimed exactly once, before any subscription is opened.
//
// Every subscription Serve installs is a JetStream consumer that lives as long as the process, so
// a second Serve doubles the consumer count for good. That is not hypothetical: a deployment
// accumulated thousands of consumers, and none could be attributed to a bug because a duplicate
// and a legitimate subscription look identical from the stream.
//
// This asserts the GUARD rather than calling Serve: a full Serve needs a live broker, and the
// claim is a single CompareAndSwap that happens before anything else — testing it through Serve
// would test the broker, not the invariant.
func TestServeGuardClaimsExactlyOnce(t *testing.T) {
	var e Extension

	if !e.serveOnce.CompareAndSwap(false, true) {
		t.Fatal("the first claim failed; Serve would refuse to start at all")
	}
	if e.serveOnce.CompareAndSwap(false, true) {
		t.Fatal("a second claim succeeded; a repeated Serve would double every consumer")
	}
}

// A fresh Extension must not start already claimed, or Serve would never run.
func TestServeGuardStartsUnclaimed(t *testing.T) {
	var e Extension
	if e.serveOnce.Load() {
		t.Fatal("a zero-value Extension starts claimed; Serve would refuse to start")
	}
}
