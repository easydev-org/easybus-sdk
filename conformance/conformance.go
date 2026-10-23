// Package conformance holds the transport-agnostic protocol test suite.
// Each transport (inproc / ws / nats) wires the same body through Run with a
// factory that returns two independent buses sharing one topology: one for
// the agent role and one for the extension role.
package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	easybus "github.com/easydev-org/easybus-sdk"
	"github.com/easydev-org/easybus-sdk/agent"
	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/extension"
	"github.com/easydev-org/easybus-sdk/protocol"
)

// T is the tenant every conformance case runs under.
const T = "t1"

// Factory returns an agent-side and an extension-side bus over one shared
// topology, plus a cleanup func.
type Factory func(t *testing.T) (agentBus bus.Bus, extBus bus.Bus, cleanup func())

const waitMs = 300

func serveEchoExt(t *testing.T, extBus bus.Bus) *extension.Extension {
	return serveEchoExtReplica(t, extBus, "r0")
}

// serveEchoExtReplica serves the conformance echo extension with an explicit
// replica id, so multi-replica cases can run N copies of the same extId.
func serveEchoExtReplica(t *testing.T, extBus bus.Bus, replicaID string) *extension.Extension {
	return serveEchoExtID(t, extBus, "conf-ext", replicaID)
}

// serveEchoExtID serves the echo extension under an explicit extId and replica id.
//
// The presence test drives a KV DELETE and waits for the agent's watcher to react.
// With a FIXED extId, an earlier run on the same broker has already created and
// deleted that exact key, so the watcher starts from a snapshot that already
// includes the history and the new delete is indistinguishable from it — the test
// then waits out its entire deadline. A fresh extId per run makes the key new.
func serveEchoExtID(t *testing.T, extBus bus.Bus, extID, replicaID string) *extension.Extension {
	t.Helper()
	ext := extension.New(extBus, extension.Config{
		ID:        extID,
		Version:   "1.0",
		ReplicaID: replicaID,
		Tools: map[string]extension.ToolSpec{
			"echo": {
				Description: "echo content",
				Execute: func(ctx context.Context, args map[string]any, callID, session, tenant string) (extension.ToolResultData, error) {
					msg, _ := args["msg"].(string)
					return extension.ToolResultData{Content: "echo:" + msg}, nil
				},
			},
			"hang": {
				Description: "waits for ctx cancellation (interrupt semantics)",
				Execute: func(ctx context.Context, args map[string]any, callID, session, tenant string) (extension.ToolResultData, error) {
					if h := hangStarted; h != nil {
						h <- callID // deterministic "call landed" signal
					}
					select {
					case <-ctx.Done():
						return extension.ToolResultData{}, fmt.Errorf("interrupted")
					case <-time.After(30 * time.Second):
						return extension.ToolResultData{Content: "never"}, nil
					}
				},
			},
			"slow": {
				Description: "sleeps before answering (request-timeout regression)",
				Execute: func(ctx context.Context, args map[string]any, callID, session, tenant string) (extension.ToolResultData, error) {
					time.Sleep(2500 * time.Millisecond)
					return extension.ToolResultData{Content: "woke"}, nil
				},
			},
			"add": {
				Description: "structured data result",
				Execute: func(ctx context.Context, args map[string]any, callID, session, tenant string) (extension.ToolResultData, error) {
					a, _ := args["a"].(float64)
					b, _ := args["b"].(float64)
					return extension.ToolResultData{Data: map[string]any{"sum": a + b}}, nil
				},
			},
			"big": {
				Description: "returns more text than the content cap allows",
				Execute: func(ctx context.Context, args map[string]any, callID, session, tenant string) (extension.ToolResultData, error) {
					big := make([]byte, 300*1024)
					for i := range big {
						big[i] = 'x'
					}
					return extension.ToolResultData{Content: string(big)}, nil
				},
			},
			"session": {
				Description: "echoes the session name it was called with",
				Execute: func(ctx context.Context, args map[string]any, callID, session, tenant string) (extension.ToolResultData, error) {
					return extension.ToolResultData{Content: "session=" + session}, nil
				},
			},
			"boom": {
				Description: "always fails",
				Execute: func(ctx context.Context, args map[string]any, callID, session, tenant string) (extension.ToolResultData, error) {
					return extension.ToolResultData{}, &extension.TypedError{Code: easybus.ToolResultErrorCodeBusiness, Message: "nope"}
				},
			},
		},
		Variables: map[string]extension.VariableSpec{
			"base-url": {
				Description: "the base url",
				Resolve: func(ctx context.Context, session, tenant string) (string, error) {
					return "https://example.com/" + session, nil
				},
			},
		},
		Config: map[string]extension.ConfigSpec{
			"poll-interval": {
				Description: "seconds between polls",
				Type:        "number",
				Default:     float64(30),
			},
			"mode": {
				Description: "operating mode",
				Type:        "enum",
				EnumValues:  []string{"fast", "safe"},
				Default:     "safe",
			},
			"session-limit": {
				Description: "per-session limit",
				Type:        "number",
				Default:     float64(10),
				Scope:       "session",
			},
			"reject-me": {
				Description: "any set is refused by the callback",
				Type:        "json",
			},
		},
		OnConfigChange: func(ctx context.Context, name string, value any, session string, get func(name, session string) any) error {
			if name == "reject-me" {
				return fmt.Errorf("config %s refused", name)
			}
			if configEvents != nil {
				configEvents <- configEvent{name: name, value: value, session: session}
			}
			return nil
		},
		CallHooks: []string{"session.before_create"},
		OnCallHook: func(ctx context.Context, hook, session string, args map[string]any, tenant string) (*easybus.HookResponse, error) {
			return &easybus.HookResponse{Ok: true, Data: easybus.Value(map[string]any{"session": session, "hook": hook})}, nil
		},
		EventHooks: []string{"session.created"},
		OnEventHook: func(ctx context.Context, hook, session string, payload any, tenant string) error {
			if extEvents != nil {
				extEvents <- event{hook: hook, session: session, payload: payload}
			}
			return nil
		},
	})
	ctx := context.Background()
	if err := ext.Serve(ctx); err != nil {
		t.Fatal(err)
	}
	return ext
}

type event struct {
	hook    string
	session string
	payload any
}

var extEvents chan event

type configEvent struct {
	name    string
	value   any
	session string
}

var configEvents chan configEvent

// Run executes the full conformance suite against one transport.
func Run(t *testing.T, newPair Factory) {
	t.Run("discover", func(t *testing.T) { testDiscover(t, newPair) })
	t.Run("tool_content", func(t *testing.T) { testToolContent(t, newPair) })
	t.Run("tool_data", func(t *testing.T) { testToolData(t, newPair) })
	t.Run("tool_object_offload", func(t *testing.T) { testToolObject(t, newPair) })
	t.Run("tool_error_mapping", func(t *testing.T) { testToolError(t, newPair) })
	t.Run("tool_call_id_echo", func(t *testing.T) { testCallID(t, newPair) })
	t.Run("tool_session_name", func(t *testing.T) { testSessionName(t, newPair) })
	t.Run("request_timeout_zero_is_bounded", func(t *testing.T) { testUnknownTool(t, newPair) })
	t.Run("slow_tool_no_request_cap", func(t *testing.T) { testSlowTool(t, newPair) })
	t.Run("interrupt_cancels_inflight_tool", func(t *testing.T) { testInterruptCancels(t, newPair) })
	t.Run("variable", func(t *testing.T) { testVariable(t, newPair) })
	t.Run("call_hook", func(t *testing.T) { testCallHook(t, newPair) })
	t.Run("event_hook", func(t *testing.T) { testEventHook(t, newPair) })
	t.Run("interrupt", func(t *testing.T) { testInterrupt(t, newPair) })
	t.Run("progress", func(t *testing.T) { testProgress(t, newPair) })
	t.Run("mailbox", func(t *testing.T) { testMailbox(t, newPair) })
	t.Run("object_store", func(t *testing.T) { testObjectStore(t, newPair) })
	t.Run("kv", func(t *testing.T) { testKV(t, newPair) })
	t.Run("config_set_and_apply", func(t *testing.T) { testConfigSet(t, newPair) })
	t.Run("config_rejected", func(t *testing.T) { testConfigRejected(t, newPair) })
	t.Run("config_validation", func(t *testing.T) { testConfigValidation(t, newPair) })
	t.Run("config_session_override", func(t *testing.T) { testConfigSession(t, newPair) })
	t.Run("config_snapshot", func(t *testing.T) { testConfigSnapshot(t, newPair) })
	t.Run("config_no_ack", func(t *testing.T) { testConfigNoAck(t, newPair) })
	t.Run("lifecycle", func(t *testing.T) { testLifecycle(t, newPair) })
	t.Run("session_events", func(t *testing.T) { testSessionEvents(t, newPair) })
	t.Run("ext_mailbox", func(t *testing.T) { testExtMailbox(t, newPair) })
	t.Run("variable_kv_first", func(t *testing.T) { testVariableKVFirst(t, newPair) })
	t.Run("session_lease", func(t *testing.T) { testSessionLease(t, newPair) })
	t.Run("term_routes_to_dlq", func(t *testing.T) { testTermDLQ(t, newPair) })
	t.Run("on_interrupt_callback", func(t *testing.T) { testOnInterrupt(t, newPair) })
	t.Run("kv_watch", func(t *testing.T) { testKVWatch(t, newPair) })
	t.Run("config_recovers_from_kv", func(t *testing.T) { testConfigKVRecovery(t, newPair) })
	t.Run("presence_liveness", func(t *testing.T) { testPresence(t, newPair) })
	t.Run("presence_replica_keys", func(t *testing.T) { testPresenceReplicas(t, newPair) })
	t.Run("hook_schema_validation", func(t *testing.T) { testHookSchema(t, newPair) })
	t.Run("config_dot_session_key", func(t *testing.T) { testConfigDotSession(t, newPair) })
	t.Run("poison_message_escalates_to_dlq", func(t *testing.T) { testPoisonEscalation(t, newPair) })
	// Separate claim, separate test: the dead letter being WRITTEN is not the same
	// as ConsumeDLQ READING it, and only the second one is broken.
	t.Run("dlq_consumer_delivers", func(t *testing.T) { testDLQConsumerDelivers(t, newPair) })
	t.Run("requeue_dlq", func(t *testing.T) { testRequeueDLQ(t, newPair) })
}

func testDiscover(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	ctx := context.Background()
	ms, err := a.Discover(ctx, waitMs)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range ms {
		if m.Id == "conf-ext" {
			found = true
		}
	}
	if !found {
		t.Fatalf("conf-ext not discovered: %+v", ms)
	}
}

func testToolContent(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	tr, err := a.CallTool(context.Background(), T, "sess-1", "conf-ext", "echo", "c1", map[string]any{"msg": "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if tr.Content != "echo:hi" {
		t.Fatalf("content = %+v", tr)
	}
}

func testToolData(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	tr, err := a.CallTool(context.Background(), T, "sess-1", "conf-ext", "add", "c2", map[string]any{"a": 2.0, "b": 3.0})
	if err != nil {
		t.Fatal(err)
	}
	m, ok := tr.Data.(map[string]any)
	if !ok {
		t.Fatalf("data = %#v", tr.Data)
	}
	if m["sum"] != 5.0 {
		t.Fatalf("sum = %#v", m["sum"])
	}
}

func testToolObject(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	// Oversized TEXT is a tool error, not a silent offload.
	//
	// This test used to assert that a tool returning 300 KiB of `content` came
	// back as an ObjectRef. Nothing ever implemented that, and the extension
	// deliberately does the opposite: `content` goes into the model's context, so
	// text above maxToolContentBytes is REJECTED with an actionable message
	// telling the tool to return a file instead (see the const's doc comment).
	// The test therefore encoded a capability that does not exist — CI is what
	// surfaced it, because this suite had never run there.
	tr, err := a.CallTool(context.Background(), T, "sess-1", "conf-ext", "big", "c3", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if tr.Error == nil {
		t.Fatalf("oversized content must be rejected, got %+v", tr)
	}
	if tr.Error.Code != easybus.ToolResultErrorCodeInvalidArgument {
		t.Errorf("code = %q, want invalid_argument", tr.Error.Code)
	}
	// The message has to say what to do instead, or the tool author is stuck.
	if !strings.Contains(tr.Error.Message, "file") {
		t.Errorf("the rejection must point at returning a file, got: %s", tr.Error.Message)
	}
}

func testToolError(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	tr, err := a.CallTool(context.Background(), T, "sess-1", "conf-ext", "boom", "c4", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if tr.Error == nil {
		t.Fatalf("expected error, got %+v", tr)
	}
	if tr.Error.Code != easybus.ErrorPayloadCodeBusiness {
		t.Fatalf("code = %q (extension maps typed errors; internal is the catch-all)", tr.Error.Code)
	}
}

func testCallID(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	tr, err := a.CallTool(context.Background(), T, "sess-1", "conf-ext", "echo", "call-xyz", map[string]any{"msg": "1"})
	if err != nil {
		t.Fatal(err)
	}
	if tr.Content != "echo:1" {
		t.Fatalf("result = %+v", tr)
	}
}

func testSessionName(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	tr, err := a.CallTool(context.Background(), T, "sess-42", "conf-ext", "session", "c5", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if tr.Content != "session=sess-42" {
		t.Fatalf("session did not propagate: %+v", tr)
	}
}

func testUnknownTool(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := a.CallTool(ctx, T, "sess-1", "conf-ext", "missing", "c6", map[string]any{}); err == nil {
		t.Fatal("expected timeout/error for unknown tool")
	}
}

func testVariable(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	v, ok := a.ResolveVariable(context.Background(), T, "sess-1", "conf-ext", "base-url")
	if !ok || v != "https://example.com/sess-1" {
		t.Fatalf("variable = %q ok=%v", v, ok)
	}
	if _, ok := a.ResolveVariable(context.Background(), T, "sess-1", "conf-ext", "nope"); ok {
		t.Fatal("missing variable should resolve null")
	}
}

func testCallHook(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	hr, err := a.CallHook(context.Background(), T, "conf-ext", "session.before_create", "sess-h", map[string]any{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	if !hr.Ok || hr.Data == nil {
		t.Fatalf("hook = %+v", hr)
	}
}

func testEventHook(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	ch := make(chan event, 8)
	extEvents = ch
	defer func() { extEvents = nil }()

	a := agent.New(agentBus)
	if err := a.PublishEventHook(context.Background(), T, "session.created", "sess-ev", map[string]any{"x": 1}); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-ch:
		if ev.hook != "session.created" || ev.session != "sess-ev" {
			t.Fatalf("event = %+v", ev)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("event hook not delivered")
	}
}

func testInterrupt(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	ch := make(chan event, 8)
	extEvents = ch
	defer func() { extEvents = nil }()

	a := agent.New(agentBus)
	if err := a.Interrupt(context.Background(), T, "conf-ext"); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-ch:
		if ev.hook != "interrupt" {
			t.Fatalf("event = %+v", ev)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("interrupt not delivered")
	}
}

func testProgress(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	ctx := context.Background()
	sub, err := agent.New(agentBus).SubscribeProgress(ctx, T, "c-progress")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := ext.ReportProgress(ctx, T, "c-progress", &easybus.ToolProgress{Phase: protocolPtr("sync"), Progress: protocolF32(0.5), Text: protocolPtr("half")}); err != nil {
		t.Fatal(err)
	}

	done := make(chan *easybus.Envelope, 1)
	go func() {
		env, ok := sub.Next(ctx)
		if ok {
			done <- env
		}
	}()
	select {
	case env := <-done:
		var p easybus.ToolProgress
		if !protocol.Coerce(env.Payload, &p) {
			t.Fatalf("payload = %#v", env.Payload)
		}
		if p.CallId != "c-progress" || p.Phase == nil || *p.Phase != "sync" {
			t.Fatalf("progress = %+v", &p)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("progress not delivered")
	}
}

// awaitMailbox scans the channel until a message for sessionName arrives
// (redeliveries from earlier tests on a shared broker are skipped).
func awaitMailbox(ch chan agent.MailboxMessageResolved, sessionName string) (agent.MailboxMessageResolved, bool) {
	deadline := time.After(3 * time.Second)
	for {
		select {
		case m := <-ch:
			if m.SessionName == sessionName {
				return m, true
			}
		case <-deadline:
			return agent.MailboxMessageResolved{}, false
		}
	}
}

func testMailbox(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	_ = extBus

	a := agent.New(agentBus)
	ctx := context.Background()

	received := make(chan agent.MailboxMessageResolved, 8)
	cancel, err := a.ConsumeMailbox(ctx, func(m agent.MailboxMessageResolved) error {
		received <- m
		return nil
	}, 50)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	time.Sleep(100 * time.Millisecond)

	session := "sess-mb-" + protocol.NewID()[:8]
	if err := a.PublishMailbox(ctx, T, session, "trigger", map[string]any{"text": "hello"}, "user"); err != nil {
		t.Fatal(err)
	}
	m, ok := awaitMailbox(received, session)
	if !ok {
		t.Fatal("mailbox message not delivered")
	}
	if m.Type != "trigger" || m.Source != "user" {
		t.Fatalf("mailbox = %+v", m)
	}
}

func testObjectStore(t *testing.T, newPair Factory) {
	agentBus, _, cleanup := newPair(t)
	defer cleanup()

	a := agent.New(agentBus)
	ctx := context.Background()
	payload := []byte("object-payload-123")
	if err := a.PutObject(ctx, T, "test-obj", payload); err != nil {
		t.Fatal(err)
	}
	got, err := a.GetObject(ctx, T, "test-obj")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("object = %q", got)
	}
	if missing, _ := a.GetObject(ctx, T, "absent-obj"); len(missing) != 0 {
		t.Fatalf("absent object = %q", missing)
	}
}

func testKV(t *testing.T, newPair Factory) {
	agentBus, _, cleanup := newPair(t)
	defer cleanup()

	a := agent.New(agentBus)
	b := a.Bus()
	ctx := context.Background()
	const bucket = "conf-kv"

	rev, err := b.KVCreate(ctx, bucket, "k", "v1", 60_000)
	if err != nil || rev == 0 {
		t.Fatalf("kvCreate = %d, %v", rev, err)
	}
	if dup, _ := b.KVCreate(ctx, bucket, "k", "v2", 60_000); dup != 0 {
		t.Fatal("kvCreate should fail on existing key")
	}
	got, _ := b.KVGet(ctx, bucket, "k")
	if got != "v1" {
		t.Fatalf("kvGet = %q", got)
	}
	cas, _ := b.KVCas(ctx, bucket, "k", "v2", rev)
	if cas == 0 {
		t.Fatal("kvCas should succeed with the right revision")
	}
	if bad, _ := b.KVCas(ctx, bucket, "k", "v3", rev); bad != 0 {
		t.Fatal("kvCas should fail with a stale revision")
	}
	got, _ = b.KVGet(ctx, bucket, "k")
	if got != "v2" {
		t.Fatalf("kvGet after cas = %q", got)
	}
	if err := b.KVDelete(ctx, bucket, "k"); err != nil {
		t.Fatal(err)
	}
	if got, _ := b.KVGet(ctx, bucket, "k"); got != "" {
		t.Fatalf("kvGet after delete = %q", got)
	}
}

func protocolPtr(s string) *string { return &s }

func protocolF32(f float64) *float32 { v := float32(f); return &v }

// serveConfAgent wires an agent with the config authority + cached manifest.
func serveConfAgent(t *testing.T, agentBus bus.Bus) *agent.Agent {
	t.Helper()
	return agent.New(agentBus)
}

func testConfigSet(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	ctx := context.Background()
	if _, err := a.Discover(ctx, waitMs); err != nil {
		t.Fatal(err)
	}
	if err := a.ServeConfig(true); err != nil {
		t.Fatal(err)
	}

	configEvents = make(chan configEvent, 8)
	defer func() { configEvents = nil }()

	if err := a.SetConfig(ctx, T, "conf-ext", "poll-interval", float64(5), "", nil, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-configEvents:
		if ev.name != "poll-interval" {
			t.Fatalf("event = %+v", ev)
		}
		if v, ok := ev.value.(float64); !ok || v != 5 {
			t.Fatalf("value = %#v", ev.value)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("config change not applied")
	}
}

func testConfigRejected(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	ctx := context.Background()
	if _, err := a.Discover(ctx, waitMs); err != nil {
		t.Fatal(err)
	}
	if err := a.ServeConfig(true); err != nil {
		t.Fatal(err)
	}

	// reject-me triggers OnConfigChange error in the fixture; undeclared on
	// the manifest? No: it must be declared. The fixture declares it via a
	// type json knob so the value validates but the callback refuses.
	err := a.SetConfig(ctx, T, "conf-ext", "reject-me", map[string]any{"x": 1}, "", nil, nil)
	if err == nil {
		t.Fatal("expected rejection")
	}
	var ce *agent.ConfigError
	if !errors.As(err, &ce) || ce.Code != "retryable" {
		t.Fatalf("err = %v", err)
	}
}

func testConfigValidation(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	ctx := context.Background()
	if _, err := a.Discover(ctx, waitMs); err != nil {
		t.Fatal(err)
	}
	if err := a.ServeConfig(true); err != nil {
		t.Fatal(err)
	}

	// wrong type
	if err := a.SetConfig(ctx, T, "conf-ext", "poll-interval", "fast", "", nil, nil); err == nil {
		t.Fatal("expected invalid_argument for wrong type")
	}
	// enum violation
	if err := a.SetConfig(ctx, T, "conf-ext", "mode", "turbo", "", nil, nil); err == nil {
		t.Fatal("expected invalid_argument for enum violation")
	}
	// undeclared name
	if err := a.SetConfig(ctx, T, "conf-ext", "nope", float64(1), "", nil, nil); err == nil {
		t.Fatal("expected not_found for undeclared config")
	}
}

func testConfigSession(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	ctx := context.Background()
	if _, err := a.Discover(ctx, waitMs); err != nil {
		t.Fatal(err)
	}
	if err := a.ServeConfig(true); err != nil {
		t.Fatal(err)
	}

	configEvents = make(chan configEvent, 8)
	defer func() { configEvents = nil }()

	if err := a.SetConfig(ctx, T, "conf-ext", "session-limit", float64(99), "sess-a", nil, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-configEvents:
		if ev.session != "sess-a" {
			t.Fatalf("session not propagated: %+v", ev)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("session config not applied")
	}

	// Drop overrides; extension keeps running.
	a.DropSessionConfig(ctx, T, "conf-ext", "sess-a")
}

func testConfigSnapshot(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	ctx := context.Background()
	if _, err := a.Discover(ctx, waitMs); err != nil {
		t.Fatal(err)
	}
	if err := a.ServeConfig(true); err != nil {
		t.Fatal(err)
	}
	if err := a.SetConfig(ctx, T, "conf-ext", "poll-interval", float64(7), "", nil, nil); err != nil {
		t.Fatal(err)
	}

	// A second extension bus pulls the snapshot and must see the set value.
	lateExt := serveLateExt(t, extBus, map[string]any{"poll-interval": float64(7)})

	_ = lateExt
}

// serveLateExt starts a second extension under the SAME id as conf-ext; the
// snapshot it pulls must contain the values set before it started.
func serveLateExt(t *testing.T, bus bus.Bus, wantGlobal map[string]any) *extension.Extension {
	t.Helper()
	ext := extension.New(bus, extension.Config{
		ID:      "conf-ext",
		Version: "1.0",
		Config: map[string]extension.ConfigSpec{
			"poll-interval": {Type: "number", Default: float64(30)},
		},
		OnConfigChange: func(ctx context.Context, name string, value any, session string, get func(name, session string) any) error {
			return nil
		},
	})
	ctx := context.Background()
	if err := ext.Serve(ctx); err != nil {
		t.Fatal(err)
	}
	// Serve() STARTS the config watch; it does not wait for the snapshot to be applied. So
	// the value can legitimately still be the declared default for a moment — asserting
	// immediately made this test fail whenever the apply landed a few microseconds later,
	// and the failure read as "got 30 want 7" (30 being the default), which looks like a
	// broken snapshot rather than a race.
	want := wantGlobal["poll-interval"]
	deadline := time.Now().Add(3 * time.Second)
	for {
		if got := ext.GetConfig(T, "poll-interval", ""); got == want {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot apply: got %v want %v (waited 3s)",
				ext.GetConfig(T, "poll-interval", ""), want)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return ext
}

func testConfigNoAck(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	time.Sleep(100 * time.Millisecond)

	a := agent.New(agentBus)
	ctx := context.Background()
	if _, err := a.Discover(ctx, waitMs); err != nil {
		t.Fatal(err)
	}
	if err := a.ServeConfig(true); err != nil {
		t.Fatal(err)
	}

	configEvents = make(chan configEvent, 8)
	defer func() { configEvents = nil }()

	noAck := false
	if err := a.SetConfig(ctx, T, "conf-ext", "poll-interval", float64(3), "", nil, &noAck); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-configEvents:
		if v, ok := ev.value.(float64); !ok || v != 3 {
			t.Fatalf("value = %#v", ev.value)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no-ack config change not applied")
	}
}

// ---------------------------------------------------------------------------
// Lifecycle / session events / ext→mailbox / KV-first variables

func testLifecycle(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()

	received := make(chan *easybus.LifecycleEvent, 8)
	ext := extension.New(extBus, extension.Config{
		ID:      "lc-ext",
		Version: "1.0",
		Variables: map[string]extension.VariableSpec{
			"ws": {Scope: "session", Resolve: func(ctx context.Context, session, tenant string) (string, error) {
				return "x", nil
			}},
		},
		Lifecycle: []string{"created", "forked", "renamed", "deleted"},
		OnLifecycle: func(ctx context.Context, ev *easybus.LifecycleEvent, tenant string) error {
			received <- ev
			return nil
		},
	})
	ctx := context.Background()
	if err := ext.Serve(ctx); err != nil {
		t.Fatal(err)
	}
	defer ext.Close()
	time.Sleep(100 * time.Millisecond)

	a := agent.New(agentBus)
	for _, kind := range []string{"created", "forked", "renamed", "deleted"} {
		if err := a.PublishLifecycleEvent(ctx, T, kind, "sess-lc", map[string]any{"k": kind}); err != nil {
			t.Fatal(err)
		}
	}
	for i, want := range []string{"created", "forked", "renamed", "deleted"} {
		select {
		case ev := <-received:
			if easybus.LifecycleKindString(ev.Kind) != want || ev.SessionName != "sess-lc" {
				t.Fatalf("event %d = %+v", i, ev)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("lifecycle %q not delivered", want)
		}
	}
}

func testSessionEvents(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()
	time.Sleep(100 * time.Millisecond)

	// The agent side replays session events by consuming the durable inbox
	// wildcard filtered to its session's events channel.
	sub, err := agentBus.InboxConsume(context.Background(), bus.InboxConsumeOpts{
		Subject: protocol.ChSessionEvents(T, "sess-se"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	time.Sleep(100 * time.Millisecond)

	if err := ext.PublishSessionEvent(context.Background(), T, "sess-se", "todos-updated", map[string]any{"count": 3}); err != nil {
		t.Fatal(err)
	}
	select {
	case msg, ok := <-func() <-chan *bus.InboxMsg {
		c := make(chan *bus.InboxMsg, 1)
		go func() {
			if m, ok := sub.Next(context.Background()); ok {
				c <- m
			}
		}()
		return c
	}():
		if !ok {
			t.Fatal("closed")
		}
		var ev struct {
			Event  string         `json:"event"`
			Params map[string]any `json:"params"`
		}
		if !protocol.Coerce(msg.Envelope.Payload, &ev) || ev.Event != "todos-updated" {
			t.Fatalf("session event = %#v", msg.Envelope.Payload)
		}
		msg.Ack()
	case <-time.After(10 * time.Second):
		t.Fatal("session event not delivered")
	}
}

func testExtMailbox(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()
	time.Sleep(100 * time.Millisecond)

	a := agent.New(agentBus)
	ctx := context.Background()
	got := make(chan agent.MailboxMessageResolved, 8)
	cancel, err := a.ConsumeMailbox(ctx, func(m agent.MailboxMessageResolved) error {
		got <- m
		return nil
	}, 50)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	time.Sleep(100 * time.Millisecond)

	session := "sess-xm-" + protocol.NewID()[:8]
	if err := ext.PublishMailboxEvent(ctx, T, session, "event", map[string]any{"done": true}); err != nil {
		t.Fatal(err)
	}
	m, ok := awaitMailbox(got, session)
	if !ok {
		t.Fatal("ext→mailbox not delivered")
	}
	if m.Type != "event" {
		t.Fatalf("mailbox = %+v", m)
	}
}

func testVariableKVFirst(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()

	// Deterministic per-run key so runs against a persistent broker don't
	// see stale cache entries from previous runs.
	session := "sess-kv-" + protocol.NewID()[:8]
	resolves := 0
	ext := extension.New(extBus, extension.Config{
		ID:      "var-ext",
		Version: "1.0",
		Variables: map[string]extension.VariableSpec{
			"ws": {Scope: "session", Resolve: func(ctx context.Context, session, tenant string) (string, error) {
				resolves++
				return "ws-" + session, nil
			}},
		},
	})
	ctx := context.Background()
	if err := ext.Serve(ctx); err != nil {
		t.Fatal(err)
	}
	defer ext.Close()
	time.Sleep(100 * time.Millisecond)

	a := agent.New(agentBus)
	// First resolve: miss → lazy request (and the ext caches via SetSessionVariable).
	v, ok := a.ResolveVariable(ctx, T, session, "var-ext", "ws")
	if !ok || v != "ws-"+session {
		t.Fatalf("resolve = %q %v", v, ok)
	}
	// Simulate the extension write-back that production resolvers do.
	if err := ext.SetSessionVariable(ctx, T, session, "ws", "ws-cached"); err != nil {
		t.Fatal(err)
	}
	// Second resolve: KV hit, no lazy request.
	v, ok = a.ResolveVariable(ctx, T, session, "var-ext", "ws")
	if !ok || v != "ws-cached" {
		t.Fatalf("cached resolve = %q %v", v, ok)
	}
	if resolves != 1 {
		t.Fatalf("lazy resolver called %d times, want 1", resolves)
	}
}

func testSessionLease(t *testing.T, newPair Factory) {
	// One shared topology: both "replicas" must see the same KV state.
	// (A second newPair would create an isolated hub/broker, where the
	// lease bucket isn't shared — the exact bug this guards against.)
	agentBus1, agentBus2, cleanup := newPair(t)
	defer cleanup()

	ctx := context.Background()
	a1 := agent.New(agentBus1)
	a2 := agent.New(agentBus2)
	session := "sess-lease-" + protocol.NewID()[:8]

	// Mutual exclusion: exactly one of two replicas claims.
	rev1, ok1 := a1.ClaimSession(ctx, T, session, 1000)
	rev2, ok2 := a2.ClaimSession(ctx, T, session, 1000)
	if !ok1 && !ok2 {
		t.Fatal("neither replica claimed the lease")
	}
	if ok1 && ok2 {
		t.Fatal("both replicas claimed the lease")
	}
	holder, rev := a1, rev1
	if ok2 {
		holder, rev = a2, rev2
	}
	if !holder.IsSessionRunning(ctx, T, session) {
		t.Fatal("lease key should exist while held")
	}

	// Renew with the right revision succeeds and bumps it.
	next, ok := holder.RenewSession(ctx, T, session, rev, 1000)
	if !ok || next <= rev {
		t.Fatalf("renew = %d %v (rev %d)", next, ok, rev)
	}
	// Renew with a stale revision loses.
	if stale, ok := holder.RenewSession(ctx, T, session, rev, 1000); ok {
		t.Fatalf("stale renew unexpectedly succeeded: %d", stale)
	}
	rev = next

	// Release → claim by the other replica succeeds.
	if err := holder.ReleaseSession(ctx, T, session); err != nil {
		t.Fatal(err)
	}
	if holder.IsSessionRunning(ctx, T, session) {
		t.Fatal("lease should be free after release")
	}
	if _, ok := a2.ClaimSession(ctx, T, session, 1000); !ok {
		t.Fatal("claim after release should succeed")
	}
	_ = a2.ReleaseSession(ctx, T, session)

	// WithSessionLease: second replica is refused while the first runs.
	done := make(chan error, 1)
	go func() {
		_, err := a1.WithSessionLease(ctx, T, session, func(ctx context.Context) error {
			acquired, err := a2.WithSessionLease(ctx, T, session, func(context.Context) error {
				return nil
			}, 1000)
			if acquired {
				t.Error("WithSessionLease: second replica acquired a held lease")
			}
			return err
		}, 1000)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("WithSessionLease did not return")
	}
}

// testSlowTool pins the request contract "TimeoutMs 0 = no transport-level
// cap": a handler slower than any internal default (2.5s > the old 2s cap)
// must still answer while the caller ctx allows it.
func testSlowTool(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tr, err := a.CallTool(ctx, T, "sess-1", "conf-ext", "slow", "slow-1", map[string]any{})
	if err != nil {
		t.Fatalf("slow tool: %v", err)
	}
	if tr.Content != "woke" {
		t.Fatalf("slow tool content = %+v", tr.Content)
	}
}

// hangStarted, when non-nil, receives the call id of every started hang
// call so tests can wait deterministically for a call to land.
var hangStarted chan string

// testInterruptCancels pins real abort semantics: a running tool of session
// X must return promptly when the agent interrupts that session, while the
// call of another session is untouched.
func testInterruptCancels(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	a := agent.New(agentBus)
	// tool errors ride in-band: an aborted call returns err==nil with
	// ToolResult.Error set ("interrupted").
	type callOutcome struct {
		tr  agent.ToolResult
		err error
	}
	mk := func(session, callID string) chan callOutcome {
		ch := make(chan callOutcome, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			tr, err := a.CallTool(ctx, T, session, "conf-ext", "hang", callID, map[string]any{})
			ch <- callOutcome{tr: tr, err: err}
		}()
		return ch
	}
	wasInterrupted := func(o callOutcome) bool {
		return o.err == nil && o.tr.Error != nil && strings.Contains(o.tr.Error.Message, "interrupted")
	}
	hangStarted = make(chan string, 2)
	defer func() { hangStarted = nil }()
	mine := mk("sess-int", "hang-1")
	other := mk("sess-other", "hang-2")
	for i := 0; i < 2; i++ { // wait until BOTH calls actually landed
		select {
		case <-hangStarted:
		case <-time.After(10 * time.Second):
			t.Fatal("hang calls never landed")
		}
	}

	if err := a.InterruptSession(context.Background(), T, "conf-ext", "sess-int", "test"); err != nil {
		t.Fatalf("interrupt session: %v", err)
	}
	select {
	case o := <-mine:
		if !wasInterrupted(o) {
			t.Fatalf("interrupted call outcome = %+v / err=%v", o.tr, o.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("session interrupt did not cancel its in-flight call within 5s")
	}
	select {
	case o := <-other:
		t.Fatalf("session-scoped interrupt must not touch other sessions (outcome %+v)", o.tr)
	default:
	}

	// broadcast interrupt reaches the remaining session
	if err := a.Interrupt(context.Background(), T, "conf-ext"); err != nil {
		t.Fatalf("interrupt broadcast: %v", err)
	}
	select {
	case o := <-other:
		if !wasInterrupted(o) {
			t.Fatalf("broadcast outcome = %+v / err=%v", o.tr, o.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("broadcast interrupt did not cancel the other call within 5s")
	}
}

// testTermDLQ pins dead-letter semantics: Term() parks the message on
// easybus.dlq.<token> in its original shape; ack/nak never land there; and
// TermNoDLQ discards outright.
func testTermDLQ(t *testing.T, newPair Factory) {
	agentBus, _, cleanup := newPair(t)
	defer cleanup()
	a := agent.New(agentBus)
	ctx := context.Background()

	sess := "sess-dlq"
	tag := "poison-" + protocol.NewID()[:6]
	_ = a.PublishMailbox(ctx, T, sess, tag, map[string]any{"bad": true})
	_ = a.PublishMailbox(ctx, T, sess, "healthy-"+tag, map[string]any{"ok": true})
	_ = a.PublishMailbox(ctx, T, sess, "discard-"+tag, map[string]any{"gone": true})

	var once sync.Once
	done := make(chan struct{})
	stop, err := a.ConsumeMailbox(ctx, func(m agent.MailboxMessageResolved) error {
		switch m.Type {
		case tag:
			return &agent.TermError{}
		case "discard-" + tag:
			return &agent.TermError{NoDLQ: true}
		default:
			if m.Type == "healthy-"+tag {
				// at-least-once: a redelivery is fine, ack it again
				once.Do(func() { close(done) })
			}
			return nil
		}
	}, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("healthy message never consumed")
	}

	// The DLQ must hold our poison message (and NOT the discard). A shared
	// broker may hold unrelated dead letters, so filter to our own tag.
	dlqDone := make(chan agent.MailboxMessageResolved, 16)
	dlqStop, err := a.ConsumeDLQ(ctx, func(m agent.MailboxMessageResolved) error {
		if m.Type == tag || m.Type == "discard-"+tag {
			dlqDone <- m
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer dlqStop()
	var found, foundDiscard bool
	deadline := time.After(10 * time.Second)
	for !found || !foundDiscard {
		select {
		case m := <-dlqDone:
			if m.Type == tag {
				found = true
			}
			if m.Type == "discard-"+tag {
				foundDiscard = true
			}
		case <-deadline:
			goto verdict
		}
	}
verdict:
	if !found {
		t.Fatal("poison message never reached the DLQ")
	}
	if foundDiscard {
		t.Fatal("TermNoDLQ message leaked into the DLQ")
	}
}

// testOnInterrupt pins the dedicated interrupt callback: it fires with the
// session and reason, taking precedence over the onEventHook fallback.
func testOnInterrupt(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()

	got := make(chan string, 1)
	hooked := make(chan string, 1)
	ext := extension.New(extBus, extension.Config{
		ID: "conf-ext", Version: "1.0",
		Tools: map[string]extension.ToolSpec{
			"hang": {Description: "hang", Execute: func(ctx context.Context, args map[string]any, callID, session, tenant string) (extension.ToolResultData, error) {
				select {
				case <-ctx.Done():
					return extension.ToolResultData{}, fmt.Errorf("interrupted")
				case <-time.After(30 * time.Second):
					return extension.ToolResultData{Content: "never"}, nil
				}
			}},
		},
		OnInterrupt: func(ctx context.Context, session, reason, tenant string) {
			select {
			case got <- session + "|" + reason:
			default:
			}
		},
		OnEventHook: func(ctx context.Context, hook, session string, payload any, tenant string) error {
			select {
			case hooked <- hook:
			default:
			}
			return nil
		},
	})
	ctx0 := context.Background()
	go func() { _ = ext.Serve(ctx0) }()
	time.Sleep(500 * time.Millisecond)
	defer ext.Close()

	a := agent.New(agentBus)
	done := make(chan error, 1)
	go func() {
		_, err := a.CallTool(ctx0, T, "sess-cb", "conf-ext", "hang", "cb-1", map[string]any{})
		done <- err
	}()
	time.Sleep(500 * time.Millisecond)
	if err := a.InterruptSession(ctx0, T, "conf-ext", "sess-cb", "because"); err != nil {
		t.Fatal(err)
	}
	select {
	case g := <-got:
		if g != "sess-cb|because" {
			t.Fatalf("OnInterrupt got %q", g)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnInterrupt never fired")
	}
	select {
	case h := <-hooked:
		t.Fatalf("onEventHook fallback fired despite OnInterrupt (%q)", h)
	case <-time.After(300 * time.Millisecond):
	}
	<-done
}

// testKVWatch pins the KV watch primitive: snapshot entries, then live
// updates, then deletes.
func testKVWatch(t *testing.T, newPair Factory) {
	agentBus, _, cleanup := newPair(t)
	defer cleanup()
	ctx := context.Background()

	// A fresh bucket per run. The presence test had the same problem: with a fixed
	// name, an earlier run on the same broker has already PUTFIELDed and DELETED
	// these exact keys, so the watch's initial snapshot already carries
	// Deleted:true and the assertions below read someone else's history. CI gets a
	// fresh broker and never sees it; a developer's broker does.
	bucket := "watch-test-" + protocol.NewID()[:6]
	_ = agentBus.KVPut(ctx, bucket, "k.a", "1", 0)
	ch, stop, err := agentBus.KvWatch(ctx, bucket, "k.>")
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	// snapshot entry
	select {
	case ev := <-ch:
		if ev.Key != "k.a" || ev.Value != "1" || ev.IsUpdate {
			t.Fatalf("snapshot event = %+v", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no snapshot event")
	}
	// snapshot/initial boundary marker arrives next
	select {
	case ev := <-ch:
		if !ev.Done {
			t.Fatalf("expected Done boundary, got %+v", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no Done boundary event")
	}
	_ = agentBus.KVPut(ctx, bucket, "k.b", "2", 0)
	select {
	case ev := <-ch:
		if ev.Key != "k.b" || ev.Value != "2" || !ev.IsUpdate || ev.Done {
			t.Fatalf("update event = %+v", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no update event")
	}
	_ = agentBus.KVDelete(ctx, bucket, "k.b")
	select {
	case ev := <-ch:
		if ev.Key != "k.b" || !ev.Deleted {
			t.Fatalf("delete event = %+v", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no delete event")
	}
}

// testConfigKVRecovery pins the 0.2 config recovery model: a set value
// survives extension restarts via the cfg KV bucket — no agent needs to be
// online or re-deliver anything.
func testConfigKVRecovery(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ctx := context.Background()

	// agent side (authority) + set one value
	a := agent.New(agentBus)
	if err := a.ServeConfig(true); err != nil {
		t.Fatal(err)
	}
	var m easybus.ExtensionManifest
	if err := json.Unmarshal([]byte(`{"id":"conf-ext","version":"1","config":[{"name":"recovered","type":"string"}]}`), &m); err != nil {
		t.Fatal(err)
	}
	if err := a.SetConfig(ctx, T, "conf-ext", "recovered", "hello-kv", "", &m, nil); err != nil {
		t.Fatalf("SetConfig: %v", err)
	}

	// extension boots AFTER the set: state must arrive via the KV watch
	ext := extension.New(extBus, extension.Config{
		ID: "conf-ext", Version: "1.0",
		Config: map[string]extension.ConfigSpec{"recovered": {Type: "string", Default: "fallback"}},
	})
	if err := ext.Serve(ctx); err != nil {
		t.Fatal(err)
	}
	defer ext.Close()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if got := ext.GetConfig(T, "recovered", ""); got == "hello-kv" {
			return // pass
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("config not recovered from KV; got %q", ext.GetConfig(T, "recovered", ""))
}

// testPresence pins the liveness model: each serving replica appears in the
// easybus-presence bucket under its own `<extId>.<replicaId>` key carrying the
// manifest. (Disappearance after Close/TTL is covered by testPresenceReplicas,
// which asserts the per-replica eviction semantics directly.)
func testPresence(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ctx := context.Background()

	ext := serveEchoExt(t, extBus)
	defer ext.Close()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if raw, _ := agentBus.KVGet(ctx, extension.PresenceBucket, "conf-ext.r0"); raw != "" {
			if !strings.Contains(raw, `"Id":"conf-ext"`) && !strings.Contains(raw, `"id":"conf-ext"`) {
				t.Fatalf("presence value not a manifest: %s", raw)
			}
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("extension never appeared in the presence bucket")
}

// testPresenceReplicas pins the MULTI-REPLICA liveness model: N replicas of one
// extId each own a distinct presence key (`<extId>.<replicaId>`), one replica
// closing deletes only its own key, and the extension drops out of the agent's
// cache only once the LAST replica key is gone.
func testPresenceReplicas(t *testing.T, newPair Factory) {
	agentBus, extBus1, cleanup := newPair(t)
	defer cleanup()
	ctx := context.Background()

	// A fresh extId per run, so the presence keys this test creates and deletes
	// have never existed on this broker before. See serveEchoExtID.
	extID := "conf-ext-" + protocol.NewID()[:6]
	keyA := extID + ".pod-a"
	keyB := extID + ".pod-b"

	// The factory hands out two buses sharing one topology. Replica A runs on
	// its own connection (so closing it never disturbs the agent's watch);
	// replica B and the agent share the other.
	r1 := serveEchoExtID(t, extBus1, extID, "pod-a")
	r2 := serveEchoExtID(t, agentBus, extID, "pod-b")
	defer r2.Close()

	a := agent.New(agentBus)
	// Cold cache -> the agent watches easybus-presence and populates from the
	// snapshot; both replica keys must land as distinct entries.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		v1, _ := agentBus.KVGet(ctx, extension.PresenceBucket, keyA)
		v2, _ := agentBus.KVGet(ctx, extension.PresenceBucket, keyB)
		if v1 != "" && v2 != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if v1, _ := agentBus.KVGet(ctx, extension.PresenceBucket, keyA); v1 == "" {
		t.Fatal("replica pod-a never appeared in the presence bucket")
	}
	if v2, _ := agentBus.KVGet(ctx, extension.PresenceBucket, keyB); v2 == "" {
		t.Fatal("replica pod-b never appeared in the presence bucket")
	}

	// Discover returns ONE manifest for the extId (de-duplicated across the two
	// replica keys).
	ms, err := a.Discover(ctx, waitMs)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, m := range ms {
		if m.Id == extID {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("Discover returned %d manifests for %s across 2 replicas, want 1", count, extID)
	}

	// Killing one replica deletes only its own key; the extension stays alive.
	if err := r1.Close(); err != nil {
		t.Fatal(err)
	}
	if v1, _ := agentBus.KVGet(ctx, extension.PresenceBucket, keyA); v1 != "" {
		t.Fatalf("closed replica's presence key still present: %q", v1)
	}
	if v2, _ := agentBus.KVGet(ctx, extension.PresenceBucket, keyB); v2 == "" {
		t.Fatal("sibling replica's presence key was evicted by the closing replica")
	}
	// Agent still sees the extension (a sibling is alive).
	if a.Manifest(extID) == nil {
		t.Fatal("extension dropped from cache while a replica was still alive")
	}

	// Last replica key gone -> the extId is offline. Delete it directly (rather
	// than closing r2's bus, which the agent shares) so the agent's watcher
	// observes the expiry on a live connection.
	if err := agentBus.KVDelete(ctx, extension.PresenceBucket, keyB); err != nil {
		t.Fatal(err)
	}
	// 15s, not 5s: this waits for a KV DELETE to propagate to a watch on a
	// different connection, which is milliseconds on a fast local broker and
	// noticeably slower on a loaded CI runner. 5s was enough to pass locally and
	// flaky on CI. The wait is still bounded — a watcher that never fires still
	// fails the test.
	deadline = time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if a.Manifest(extID) == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("extension still cached after the last replica went away")
}

// testConfigDotSession pins the KV key escaping: a session name containing
// dots must roundtrip through the cfg KV bucket (watch-side parsing must
// not split on the dots inside the session segment).
func testConfigDotSession(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ctx := context.Background()

	a := agent.New(agentBus)
	if err := a.ServeConfig(true); err != nil {
		t.Fatal(err)
	}
	var m easybus.ExtensionManifest
	if err := json.Unmarshal([]byte(`{"id":"conf-ext","version":"1","config":[{"name":"dots","type":"string","scope":"session"}]}`), &m); err != nil {
		t.Fatal(err)
	}
	sess := "weird.session.name:main"
	if err := a.SetConfig(ctx, T, "conf-ext", "dots", "escaped-ok", sess, &m, nil); err != nil {
		t.Fatalf("SetConfig: %v", err)
	}

	ext := extension.New(extBus, extension.Config{
		ID: "conf-ext", Version: "1.0",
		Config: map[string]extension.ConfigSpec{"dots": {Type: "string", Scope: "session"}},
	})
	if err := ext.Serve(ctx); err != nil {
		t.Fatal(err)
	}
	defer ext.Close()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if got := ext.GetConfig(T, "dots", sess); got == "escaped-ok" {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("dot-session config not recovered; got %v", ext.GetConfig(T, "dots", sess))
}

// testPoisonEscalation pins the default-safety behavior: a handler that keeps
// failing gets the message redelivered at most maxNaks times, and the message is
// then PARKED as a dead letter — no infinite nak loop, and no silent loss.
//
// It deliberately stops at "the dead letter exists in the DLQ stream", read back
// through a plain subscription rather than through ConsumeDLQ. Those are two
// different claims and only one of them holds today:
//
//	(a) the dead letter reaches the DLQ stream      <- this test
//	(b) ConsumeDLQ hands it to a handler           <- TestDLQConsumerDelivers
//
// Conflating them produced a misleading failure ("poison message never landed in
// the DLQ") for a message that WAS in the stream: the real defect is on the read
// side, and the message needs to say so.
// drainStreams clears the mailbox and DLQ streams so a test does not inherit the
// broker's history.
//
// InboxConsume starts at DeliverAll by design. That is right for production (a
// consumer that starts late must still see queued work) and wrong for a test on a
// shared broker: the handler spends its budget re-nakking yesterday's dead letters
// while this run's message waits behind them, and the failure presents as a broken
// DLQ. CI has a fresh broker per job and never sees it.
func drainStreams(t *testing.T, ctx context.Context, b bus.Bus) {
	t.Helper()
	for _, ch := range []string{protocol.MailboxWildcardAll, "easybus.*.dlq"} {
		if err := b.InboxPurge(ctx, ch); err != nil {
			t.Fatalf("purge %s: %v", ch, err)
		}
	}
}

func testPoisonEscalation(t *testing.T, newPair Factory) {
	agentBus, _, cleanup := newPair(t)
	defer cleanup()
	ctx := context.Background()
	a := agent.New(agentBus)
	tag := "always-fails-" + protocol.NewID()[:6]

	// Drain whatever earlier runs left in these streams.
	//
	// InboxConsume starts at DeliverAll on purpose — a consumer that starts late
	// must still see queued work, and for the DLQ that is the whole point of an ops
	// surface. But it means a test run inherits every message from previous runs on
	// the same broker, and a shared broker is exactly what a developer has. The
	// observable effect is nasty: the handler spends its time re-nakking dead
	// letters from LAST week while this test's message sits behind them, and the
	// failure reads as "the DLQ is broken" when it is the test that is not isolated.
	//
	// CI never sees this (fresh broker per job), so it has to be fixed here.
	drainStreams(t, ctx, agentBus)

	// Watch the DLQ raw, before anything is published: the inbox bus is core NATS
	// pub/sub, so a subscriber that arrives later misses what already went by.
	raw, err := agentBus.InboxConsume(ctx, bus.InboxConsumeOpts{Subject: "easybus.*.dlq.>"})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	landed := make(chan string, 8)
	go func() {
		for {
			msg, ok := raw.Next(ctx)
			if !ok {
				return
			}
			var m easybus.MailboxMessage
			if msg.Envelope != nil && protocol.Coerce(msg.Envelope.Payload, &m) && m.Type == tag {
				select {
				case landed <- m.Type:
				default:
				}
			}
			msg.Ack()
		}
	}()

	_ = a.PublishMailbox(ctx, T, "sess-poison", tag, map[string]any{"n": 1})

	deliveries := 0
	done := make(chan int, 1)
	stop, err := a.ConsumeMailbox(ctx, func(m agent.MailboxMessageResolved) error {
		deliveries++
		if deliveries >= 3 {
			done <- deliveries
		}
		return fmt.Errorf("boom %d", deliveries)
	}, 50, 3) // maxNaks=3 for a fast test
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	select {
	case <-done:
		select {
		case got := <-landed:
			if got != tag {
				t.Fatalf("DLQ type = %q, want %q", got, tag)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("the dead letter never reached the DLQ stream (message %q)", tag)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("message did not reach maxNaks deliveries")
	}
}

// TestDLQConsumerDelivers is claim (b) from the comment above: ConsumeDLQ — the
// ops surface for triage — must hand a handler the dead letters that arrive while
// it is watching.
//
// KNOWN FAILURE, and it is a REAL defect, not a test artifact. Measured: the
// consumer drains whatever was already in the stream (15 messages from earlier
// runs) and then stops delivering; a dead letter published afterwards sits in the
// stream but never reaches the handler (confirmed by reading the same stream with
// a plain consumer, which does see it).
//
// The mechanism is not yet pinned down. Two candidates, and one query tells them
// apart: consumer NumPending == 0 while messages remain (the pull loop thinks it
// is drained) versus NumAckPending == MaxAckPending (unacked messages are blocking
// delivery).
func testDLQConsumerDelivers(t *testing.T, newPair Factory) {
	agentBus, _, cleanup := newPair(t)
	defer cleanup()
	ctx := context.Background()
	a := agent.New(agentBus)

	// Same isolation reasoning as testPoisonEscalation.
	drainStreams(t, ctx, agentBus)

	seen := make(chan string, 16)
	stop, err := a.ConsumeDLQ(ctx, func(m agent.MailboxMessageResolved) error {
		select {
		case seen <- m.Type:
		default:
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	// The subscription is established asynchronously; give it a beat so the test
	// fails on DELIVERY, not on a subscribe race.
	time.Sleep(500 * time.Millisecond)

	tag := "dlq-deliver-" + protocol.NewID()[:6]
	_ = a.PublishMailbox(ctx, T, "sess-poison-2", tag, map[string]any{"n": 1})

	// Poison it so it is parked: three failures, then Term.
	var n int
	stopMb, err := a.ConsumeMailbox(ctx, func(m agent.MailboxMessageResolved) error {
		n++
		return fmt.Errorf("boom %d", n)
	}, 50, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer stopMb()

	deadline := time.After(20 * time.Second)
	for {
		select {
		case got := <-seen:
			if got == tag {
				return
			}
		case <-deadline:
			t.Fatalf("ConsumeDLQ never delivered %q (it may have seen others; "+
				"see the doc comment for the measured behaviour)", tag)
		}
	}
}

// testRequeueDLQ pins the DLQ return path: term -> triage -> requeue ->
// consumed successfully on the second life.
func testRequeueDLQ(t *testing.T, newPair Factory) {
	agentBus, _, cleanup := newPair(t)
	defer cleanup()
	ctx := context.Background()
	a := agent.New(agentBus)

	id := protocol.NewID()
	if err := agentBus.InboxPublish(ctx, protocol.ChMailbox(T, "sess-rq"), easybus.MailboxMessage{Id: id, Type: "requeue-me", Payload: easybus.Value(map[string]any{"k": 1})}, bus.InboxPublishOpts{ID: id, SessionName: "sess-rq", Tenant: T}); err != nil {
		t.Fatal(err)
	}

	succeeded := make(chan agent.MailboxMessageResolved, 1)
	firstLife := true // first delivery parks in the DLQ; the requeued copy succeeds
	stop, err := a.ConsumeMailbox(ctx, func(m agent.MailboxMessageResolved) error {
		if m.Type == "requeue-me" && !firstLife {
			succeeded <- m
			return nil
		}
		if m.Type == "requeue-me" {
			firstLife = false
		}
		return &agent.TermError{} // park it
	}, 100, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	// wait for the dead letter, then requeue it
	var dlqID string
	deadline := time.Now().Add(10 * time.Second)
	for dlqID == "" && time.Now().Before(deadline) {
		dlqStop, err := a.ConsumeDLQ(ctx, func(m agent.MailboxMessageResolved) error {
			if m.Type == "requeue-me" && dlqID == "" {
				dlqID = m.ID
			}
			return fmt.Errorf("keep") // nak so it stays available to RequeueDLQ
		})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(500 * time.Millisecond)
		dlqStop()
	}
	if dlqID == "" {
		t.Fatal("message never reached the DLQ")
	}
	ok, err := a.RequeueDLQ(ctx, dlqID)
	if err != nil || !ok {
		t.Fatalf("RequeueDLQ: ok=%v err=%v", ok, err)
	}
	select {
	case m := <-succeeded:
		if m.Type != "requeue-me" {
			t.Fatalf("requeued type = %q", m.Type)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("requeued message never consumed")
	}
}

// testHookSchema pins the declarative hook payload schemas: an
// out-of-contract call hook argument is rejected in-band; an out-of-contract
// event hook payload is dropped (best-effort).
func testHookSchema(t *testing.T, newPair Factory) {
	agentBus, extBus, cleanup := newPair(t)
	defer cleanup()
	ctx := context.Background()

	badCall := make(chan easybus.HookResponse, 1)
	badEvent := make(chan string, 1)
	ext := extension.New(extBus, extension.Config{
		ID: "conf-ext", Version: "1.0",
		CallHooks:  []string{"audit"},
		EventHooks: []string{"notice"},
		HookSchemas: extension.HookSchemas{
			Call:  map[string]map[string]any{"audit": {"type": "object", "required": []any{"who"}, "properties": map[string]any{"who": map[string]any{"type": "string"}}}},
			Event: map[string]map[string]any{"notice": {"type": "object", "required": []any{"kind"}, "properties": map[string]any{"kind": map[string]any{"type": "string"}}}},
		},
		OnCallHook: func(ctx context.Context, hook, session string, args map[string]any, tenant string) (*easybus.HookResponse, error) {
			badCall <- easybus.HookResponse{Ok: true} // would only reach here with valid args
			return &easybus.HookResponse{Ok: true}, nil
		},
		OnEventHook: func(ctx context.Context, hook, session string, payload any, tenant string) error {
			badEvent <- "delivered"
			return nil
		},
	})
	go func() { _ = ext.Serve(context.Background()) }()
	time.Sleep(500 * time.Millisecond)
	defer ext.Close()

	a := agent.New(agentBus)
	// bad call args: missing "who" -> in-band invalid_argument
	r, err := a.CallHook(ctx, T, "conf-ext", "audit", "sess-h", map[string]any{"nope": true})
	if err != nil {
		t.Fatal(err)
	}
	if r.Ok || r.Error == nil || r.Error.Code != easybus.HookResponseErrorCodeInvalidArgument {
		t.Fatalf("bad call args: got ok=%v err=%+v", r.Ok, r.Error)
	}
	// bad event payload: missing "kind" -> dropped (no delivery)
	if err := a.PublishEventHook(ctx, T, "notice", "sess-h", map[string]any{"unknown": 1}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-badEvent:
		t.Fatal("invalid event payload was delivered")
	case <-time.After(500 * time.Millisecond):
	}
	// valid payloads still flow
	if err := a.PublishEventHook(ctx, T, "notice", "sess-h", map[string]any{"kind": "ok"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-badEvent:
	case <-time.After(5 * time.Second):
		t.Fatal("valid event payload not delivered")
	}
	select {
	case <-badCall:
	default:
	}
}
