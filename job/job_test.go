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

/**
 * TWO KINDS OF CANCELLING, BECAUSE THEY ARE TWO DIFFERENT FACTS.
 *
 * `kill` ends a process this extension owns: the signal lands and the call's answer is the answer.
 * `request` asks somebody else's system to stop, and that system may decline or take a step to
 * comply. A caller told only "cancel" would believe a CI run had stopped when it is merely stopping.
 */
func TestCancelModesAreDistinct(t *testing.T) {
	if CancelKill == CancelRequest {
		t.Fatal("kill and request must not share a value; they promise different things")
	}
	if !CancelKill.Cancelable() || !CancelRequest.Cancelable() {
		t.Fatal("both kill and request are worth attempting")
	}
	// THE ZERO VALUE IS THE HONEST DEFAULT: a job that did not say it can be cancelled cannot be.
	var unset CancelMode
	if unset != CancelNone {
		t.Fatalf("the zero CancelMode is %q, not CancelNone", unset)
	}
	if unset.Cancelable() {
		t.Fatal("an unset cancel mode must not offer cancellation")
	}
	if (CancelMode("something-new")).Cancelable() {
		t.Fatal("an unrecognised mode must not offer cancellation")
	}
}

func TestEntryCarriesItsCancelMode(t *testing.T) {
	// THE MODE SURVIVES THE WIRE, which is the only way a client can decide whether to draw a
	// cancel button. A json tag typo would silently make every job look uncancellable.
	in := Entry{ID: "ci-run.7", CancelMode: CancelRequest}
	raw, err := marshalEntry(in)
	if err != nil {
		t.Fatal(err)
	}
	var out Entry
	if err := unmarshalEntry(string(raw), &out); err != nil {
		t.Fatal(err)
	}
	if out.CancelMode != CancelRequest {
		t.Fatalf("cancel mode came back as %q", out.CancelMode)
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

/**
 * A KEY BUILT FROM THE WRONG FIELD IS INVISIBLE, NOT WRONG-LOOKING.
 *
 * The bucket key is `t.<tenant>.s.<session>.<id>`. Building it from an entry's `ExtID` produces a
 * valid key under a tenant nobody queries: the job registers, renews and finishes, and the list that
 * asks for `t.<tenant>.s.<session>.*` never sees it. Nothing logs, nothing errors — the only symptom
 * is an empty list beside a task the reader watched run.
 *
 * THIS TEST EXISTS BECAUSE THAT HAPPENED: `Key(e.ExtID, e.Session, e.ID)` and
 * `SessionPrefix(tenant, session)` were both "right" on their own.
 */
func TestWriteKeyAndReadPrefixAgree(t *testing.T) {
	e := Entry{Tenant: "alice", ExtID: "sandbox", Session: "s1", ID: "sandbox-job.abc"}
	written := Key(e.Tenant, e.Session, e.ID)
	prefix := SessionPrefix(e.Tenant, e.Session)

	// THE WRITE KEY MUST BE INSIDE THE PREFIX the reader asks for.
	stem := prefix[:len(prefix)-1] // strip the trailing "*"
	if len(written) <= len(stem) || written[:len(stem)] != stem {
		t.Fatalf("a job written at %q is not visible under %q", written, prefix)
	}

	// AND THE EXTENSION ID MUST NOT APPEAR WHERE THE TENANT BELONGS — that is the exact mistake, and
	// it is spelled out rather than implied so the failure names the cause.
	wrong := Key(e.ExtID, e.Session, e.ID)
	if wrong == written {
		t.Fatal("the tenant and ext id produced the same key; the test cannot tell them apart")
	}
	if len(wrong) > len(stem) && wrong[:len(stem)] == stem {
		t.Fatal("a key built from the ext id fell inside the tenant prefix; the segments overlap")
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
