package job

import (
	"testing"
	"time"
)

/**
 * The timings are arithmetic, not taste, and these tests pin the arithmetic.
 *
 * EVERY ONE OF THEM IS A PROPERTY THAT BREAKS SILENTLY IF SOMEBODY TUNES A CONSTANT BY
 * FEEL: a running TTL shorter than the staleness threshold makes lost jobs vanish instead
 * of being recorded; a renewal interval too close to the TTL makes one slow call look like
 * a death.
 */

func TestRunningTTLOutlastsTheStalenessThreshold(t *testing.T) {
	r := NewRegistry(nil)
	// THE SWEEPER JUDGES A PRESENT ENTRY. If the entry expired first there would be
	// nothing to judge, and the job would disappear rather than being marked lost —
	// which is the one outcome the whole design is arranged to avoid.
	if r.RunningTTL <= r.StaleAfter {
		t.Fatalf("running TTL %v must exceed stale-after %v, or a stalled job disappears "+
			"before it can be recorded as lost", r.RunningTTL, r.StaleAfter)
	}
}

func TestSeveralRenewalsFitInsideOneLifetime(t *testing.T) {
	r := NewRegistry(nil)
	// ONE MISSED RENEWAL MUST NOT BE A DEATH. Three consecutive failures should be
	// survivable before the entry lapses, so a slow bus call is not a lost job.
	if r.RunningTTL/r.RenewEvery < 3 {
		t.Fatalf("only %d renewals fit in %v at %v apart; a single slow call would lapse the entry",
			r.RunningTTL/r.RenewEvery, r.RunningTTL, r.RenewEvery)
	}
}

func TestFinishedEntriesOutliveRunningOnes(t *testing.T) {
	r := NewRegistry(nil)
	// A FINISHED JOB IS HISTORY, A RUNNING ONE IS STATE. The first is worth reading for
	// a day; the second is worth exactly as long as somebody keeps saying it is alive.
	if r.DoneTTL <= r.RunningTTL {
		t.Fatalf("finished TTL %v must exceed running TTL %v", r.DoneTTL, r.RunningTTL)
	}
	if r.DoneTTL != 24*time.Hour {
		t.Fatalf("finished TTL is %v, and the product decision is 24h", r.DoneTTL)
	}
}

func TestStalenessIsAtLeastTwoRenewalIntervals(t *testing.T) {
	r := NewRegistry(nil)
	// ONE LATE RENEWAL IS TOLERATED, TWO ARE NOT. Below two intervals a single delayed
	// call would make a live job look dead.
	if r.StaleAfter < 2*r.RenewEvery {
		t.Fatalf("stale-after %v is under two renewal intervals (%v); one delayed call would read as death",
			r.StaleAfter, 2*r.RenewEvery)
	}
}

func TestTerminalStatesAreTerminal(t *testing.T) {
	for _, s := range []State{StateDone, StateCancelled, StateLost} {
		if !s.Terminal() {
			t.Errorf("%s must be terminal", s)
		}
	}
	if StateRunning.Terminal() {
		t.Error("running must not be terminal, or nothing would ever be renewed")
	}
	// AN UNKNOWN STATE IS TREATED AS TERMINAL: renewing something this code does not
	// understand would keep it alive forever.
	if !State("something-new").Terminal() {
		t.Error("an unrecognised state must be treated as terminal")
	}
}

/**
 * THERE IS NO `failed` STATE, AND THAT IS THE POINT.
 *
 * A job that exited 1 is `done` with ExitCode 1. The job knows the code; the caller knows what it
 * wanted — `grep` exiting 1 means "no match", which is a successful search and a failed check. A
 * state named after the exit code would decide that on the caller's behalf.
 *
 * THIS TEST EXISTS TO STOP THE OBVIOUS "IMPROVEMENT" of splitting `done` into succeeded/failed.
 */
func TestDoneDoesNotDistinguishSuccessFromFailure(t *testing.T) {
	if State("failed") == StateDone || State("succeeded") == StateDone {
		t.Fatal("state vocabulary must not gain a success/failure split")
	}
	// The exit code is where that answer lives, and it is optional precisely because a cancelled
	// or lost job has none.
	e := Entry{State: StateDone}
	if e.ExitCode != nil {
		t.Fatal("a done job's exit code starts unset and is filled in by whoever ran it")
	}
}

func TestKeySegmentsOrderFromGeneralToSpecific(t *testing.T) {
	// THE ORDER IS WHAT MAKES A WILDCARD USEFUL: a panel asks for one conversation, a
	// sweeper asks for one tenant, and both are prefixes of the same shape.
	got := Key("alice", "s1", "sandbox.run.abc")
	want := "t.alice.s.s1.sandbox.run.abc"
	if got != want {
		t.Fatalf("Key = %q, want %q", got, want)
	}
	if prefix := SessionPrefix("alice", "s1"); prefix != "t.alice.s.s1.*" {
		t.Fatalf("SessionPrefix = %q", prefix)
	}
}

func TestEntryRoundTripsThroughTheOneEncoder(t *testing.T) {
	exit := 3
	in := Entry{
		ID: "sandbox.run.abc", Kind: "shell", Title: "cargo build",
		State: StateDone, ExitCode: &exit,
		StartedAt: 1000, FinishedAt: 2000, RenewedAt: 2000,
		ExtID: "sandbox", Session: "s1", ToolUseID: "tool_1",
	}
	raw, err := marshalEntry(in)
	if err != nil {
		t.Fatal(err)
	}
	var out Entry
	if err := unmarshalEntry(string(raw), &out); err != nil {
		t.Fatal(err)
	}
	if out.ID != in.ID || out.State != in.State || out.ToolUseID != in.ToolUseID {
		t.Fatalf("round trip lost fields: %+v", out)
	}
	if out.ExitCode == nil || *out.ExitCode != exit {
		t.Fatalf("exit code did not survive: %v", out.ExitCode)
	}
}
