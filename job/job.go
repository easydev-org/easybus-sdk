// Package job is the background-job abstraction.
//
// A TOOL HAS TWO SHAPES, AND THEY ARE NOT THE SAME SHAPE:
//
//	SYNCHRONOUS  Execute returns, the call is over, the result is that return value.
//	ASYNCHRONOUS the call returns as soon as the work STARTS. The result arrives later,
//	             as a completion notice, long after the tool call is history.
//
// Trying to express the second with the first is what this package exists to avoid: a
// tool that blocks for forty minutes is a tool that looks broken, holds the turn open,
// and dies with the process that started it.
//
// WHAT LIVES HERE IS ONLY THE VOCABULARY — State, Entry, the IDs. The storage is the
// bus (a KV bucket), the output is the extension's own tool-output stream, and the
// completion notice is the extension's own event. Nothing in this package imports the
// bus, so an extension can use these types without taking on the transport.
package job

/**
 * State says WHY a job stopped, and nothing else.
 *
 * THERE IS NO "failed". A job that exited 1 is `done` with ExitCode 1 — the state reports what
 * ended, the exit code reports what the ending was worth. Collapsing the two would make the job
 * decide success on the caller's behalf, and it cannot: `grep` exiting 1 means "no match", which
 * is a successful search to one caller and a failed check to another. The job knows the code; the
 * caller knows what it wanted.
 *
 * FOUR STATES, AND ONLY ONE OF THEM IS NOT AN ENDING. There is no "pending" or "queued" either: a
 * job exists only once something has begun running it, so a state describing a job that has not
 * started would name a thing this system never holds.
 */
type State string

const (
	// StateRunning is the only non-terminal state, and the only one that is renewed.
	StateRunning State = "running"
	/**
	 * StateDone means the process ended on its own. ExitCode says how.
	 *
	 * A NON-ZERO EXIT IS STILL `done`. The alternative — naming the state after the exit code —
	 * puts the judgement in the wrong place and leaves a state (`completed`) that can contradict
	 * its own exit code.
	 */
	StateDone State = "done"
	// StateCancelled means something stopped it on purpose. Nothing produces this yet:
	// cancellation is not in the first version, and the constant is here so the wire contract does
	// not have to grow a second version to add it.
	StateCancelled State = "cancelled"
	// StateLost means the extension running it stopped renewing, so no exit code exists.
	//
	// IT IS WRITTEN BY SOMEBODY ELSE. The extension cannot record its own death, so a sweeper that
	// observes a stalled heartbeat records this in its place — see Terminated() for the judgement
	// it makes.
	StateLost State = "lost"
)

// Terminal reports whether a state is an end.
//
// USED TO DECIDE WHETHER TO RENEW. A terminal job is not renewed, which is what lets its
// entry age out on the long TTL instead of the short one.
func (s State) Terminal() bool {
	switch s {
	case StateRunning:
		return false
	case StateDone, StateCancelled, StateLost:
		return true
	}
	// An unknown state is treated as terminal: renewing something this code does not
	// understand would keep it alive forever, which is the more expensive mistake.
	return true
}

/**
 * Entry is one job as the rest of the system sees it.
 *
 * IT CARRIES NO OUTPUT. A build can emit tens of megabytes and a KV value cannot hold
 * that; the output is the extension's tool-output stream, which is already on the wire
 * and already understood by every client. What is here is what a LIST needs: enough to
 * draw a row and to find the job again.
 *
 * IT ALSO CARRIES NO FREE-FORM BAG. An earlier draft had `Progress map[string]any`; it
 * was removed because a field with no schema becomes a place for each extension to put
 * its own vocabulary, and a client cannot render a vocabulary it has never seen. If
 * progress is needed later it should be a shaped field with shared meaning, not a map.
 */
type Entry struct {
	// ID is unique across extensions: the extension prefixes its own identifier.
	ID string `json:"id"`
	// Kind is the extension's word for what sort of work this is ("shell", "build",
	// "ci", "browser"). Presentation only — nothing branches on it in the core.
	Kind string `json:"kind"`
	// Title is one line for a human: the command, the build target, the workflow name.
	Title string `json:"title"`

	State State `json:"state"`
	// ExitCode is set once the work ends. Nil while running, and nil for a job that
	// never produced one (lost, or killed by a signal).
	ExitCode *int `json:"exitCode,omitempty"`

	// StartedAt is Unix milliseconds.
	StartedAt int64 `json:"startedAt"`
	// FinishedAt is Unix milliseconds, 0 while running.
	FinishedAt int64 `json:"finishedAt,omitempty"`
	/**
	 * RenewedAt is Unix milliseconds of the last heartbeat, and it is what a sweeper
	 * judges staleness by.
	 *
	 * IT IS SEPARATE FROM StartedAt BECAUSE A JOB CAN RUN FOR HOURS: "this has been
	 * going a long time" and "nobody is looking after this any more" are different
	 * questions, and only the second one means the job is gone.
	 */
	RenewedAt int64 `json:"renewedAt"`

	// ExtID is the extension that owns it — the only process that can renew it.
	ExtID string `json:"extId"`
	// Session is the conversation that started it, so a client can list one
	// conversation's jobs rather than the tenant's whole history.
	Session string `json:"session"`

	/**
	 * Tenant is whose configuration the work belongs to.
	 *
	 * IT IS SEPARATE FROM ExtID AND IT HAS TO BE HERE. The bucket key is `t.<tenant>.s.<session>.<id>`,
	 * and the writer is the only place that knows all three — an entry without its tenant cannot be
	 * keyed correctly, and a key built from the wrong field is invisible to every reader rather than
	 * obviously wrong: the job registers, renews, and finishes, while the list that asks for
	 * `t.<tenant>.s.<session>.*` never sees it.
	 */
	Tenant string `json:"tenant"`

	/**
	 * ToolUseID ties the job back to the tool call that produced it.
	 *
	 * THE CALL RETURNS BEFORE THE JOB DOES, so by the time a completion notice arrives
	 * the call is no longer in scope. This is the only handle the agent has for "which
	 * turn was waiting on this", and without it a finished job has nowhere to report.
	 */
	ToolUseID string `json:"toolUseId,omitempty"`

	// Reason explains a terminal state that needs one — why it was lost, or what the
	// failure was. Empty when the state speaks for itself.
	Reason string `json:"reason,omitempty"`

	/**
	 * CancelMode says how — or whether — cancelling this job can do anything.
	 *
	 * THREE VALUES BECAUSE TWO FACTS ARE NOT THE SAME. A process this extension owns can be KILLED;
	 * a run submitted to somebody else's system can only be ASKED to stop, and that request may be
	 * declined or may take a step to take effect. A caller that treats both as "cancel" will believe
	 * a CI run stopped when it is merely stopping.
	 *
	 * IT IS A PROPERTY OF THE KIND OF JOB, NOT OF ITS STATE. A running job may still be uncancellable
	 * — a build sitting in a queue has not started and has nothing to stop.
	 */
	CancelMode CancelMode `json:"cancelMode,omitempty"`
}

// CancelMode is how a job can be stopped.
type CancelMode string

const (
	// CancelKill means this extension owns the process and can end it: the signal lands, and the
	// answer to "did it stop" is the answer to the call.
	CancelKill CancelMode = "kill"
	/**
	 * CancelRequest means the work belongs to a system that has to be ASKED.
	 *
	 * A GITHUB RUN IS THIS: cancelling sets a flag, and the step in progress finishes before the
	 * run stops. A caller told "cancelled" here would believe the machine had stopped, when what it
	 * has is a request in flight — which is why the mode travels with the job rather than being
	 * flattened into a boolean.
	 */
	CancelRequest CancelMode = "request"
	// CancelNone means there is nothing to stop: the work has not started, or it runs somewhere
	// this extension cannot reach. Empty so a zero value is the honest default — a job that did not
	// say it can be cancelled cannot be.
	CancelNone CancelMode = ""
)

// Cancelable reports whether cancelling is worth attempting.
func (m CancelMode) Cancelable() bool { return m == CancelKill || m == CancelRequest }
