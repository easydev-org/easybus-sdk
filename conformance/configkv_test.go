package conformance

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/extension"
	"github.com/easydev-org/easybus-sdk/natsrun"
	"github.com/easydev-org/easybus-sdk/protocol"
	"github.com/easydev-org/easybus-sdk/transport/nats"
)

// These three cases cover the KV-recovery path: the only way a RESTARTED
// extension learns its configuration.
//
// The existing suite exercises `SetConfig`, which is the live-push path. The two
// are not interchangeable — the push notifies a running extension, while the KV
// snapshot is what a fresh process reads at startup — and every defect found in
// the real stack lived on the KV side.
//
// The same three cases exist in the Python, Rust and TypeScript SDKs. They are
// duplicated rather than generated because each implementation owns its own
// decoder; what has to agree is the BEHAVIOUR, and only a shared set of cases can
// pin that down.

// TestConfigKVBareValueIsStored: a knob's value is ultimately a string.
//
// The agent writes it verbatim, and a URL or a token is not JSON — so a decoder
// that requires JSON drops the entry, silently, one key at a time. The extension
// then reports every knob unset while the agent and the console both insist the
// value is saved.
func TestConfigKVBareValueIsStored(t *testing.T) {
	withKV(t, "t.conf-t1.ep-conf.endpoint", "http://easyops.example:80",
		map[string]extension.ConfigSpec{"endpoint": {Type: "string"}},
		"endpoint", "http://easyops.example:80")
}

// TestConfigKVStructuredValueStaysText: a list/object knob arrives as its ORIGINAL
// TEXT.
//
// The knob carries a declaration the extension parses itself. Handing back a
// parsed slice makes a string-typed reader see nothing, so the knob reads as unset
// even though the value was delivered — and every sandbox/computer tool requires a
// structured `sandboxes` knob, so this is the common case.
func TestConfigKVStructuredValueStaysText(t *testing.T) {
	const raw = `[{"name":"local","url":"http://worker:80"}]`
	withKV(t, "t.conf-t1.ep-conf.servers", raw,
		map[string]extension.ConfigSpec{"servers": {Type: "list"}},
		"servers", raw)
}

// TestConfigKVEnvelopeStillUnwraps: the legacy `{"r":<rev>,"v":<value>}` envelope
// keeps working.
//
// Guards the other direction: accepting bare values must not break an agent that
// still writes the envelope.
func TestConfigKVEnvelopeStillUnwraps(t *testing.T) {
	withKV(t, "t.conf-t1.ep-conf.poll", `{"r":9,"v":77}`,
		map[string]extension.ConfigSpec{"poll": {Type: "number"}},
		"poll", float64(77))
}

// TestConfigKVBucketName pins a cross-process contract.
//
// Extensions subscribe to `t.*.<extId>.>` in this bucket. An agent writing to a
// differently-named bucket stores every value where nothing is looking, and each
// side still looks healthy on its own — which is exactly how it went unnoticed.
func TestConfigKVBucketName(t *testing.T) {
	if protocol.ConfigKVBucket != "cfg" {
		t.Fatalf("ConfigKVBucket = %q, want cfg", protocol.ConfigKVBucket)
	}
}

// withKV seeds one key, starts an extension, and asserts the knob resolves.
func withKV(t *testing.T, key, value string, specs map[string]extension.ConfigSpec, knob string, want any) {
	t.Helper()
	agentBus, extBus, cleanup := freshPair(t)
	defer cleanup()

	ctx := context.Background()
	if err := agentBus.KVPut(ctx, protocol.ConfigKVBucket, key, value, 0); err != nil {
		t.Fatalf("seed kv: %v", err)
	}

	ext := extension.New(extBus, extension.Config{ID: "ep-conf", Version: "1", Config: specs})
	if err := ext.Serve(ctx); err != nil {
		t.Fatalf("serve: %v", err)
	}
	defer ext.Close()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got := ext.GetConfig("conf-t1", knob, ""); equalValue(got, want) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s = %#v, want %#v (the value was seeded in the bucket extensions watch)",
		knob, ext.GetConfig("conf-t1", knob, ""), want)
}

// freshPair starts its own broker and returns two connections (agent + extension).
//
// A dedicated broker per case: a shared one leaks queue-group subscriptions across
// tests during async teardown, which makes KV-recovery assertions flaky — the exact
// class of failure these cases exist to catch.
func freshPair(t *testing.T) (bus.Bus, bus.Bus, func()) {
	t.Helper()
	url := os.Getenv("EASYBUS_NATS_URL")
	var stopServer func()
	if url == "" {
		s, err := natsrun.Start(natsrun.Config{Storage: natsrun.Memory})
		if err != nil {
			t.Skip("no nats-server available (install nats-server or set EASYBUS_NATS_SERVER_BIN or EASYBUS_NATS_URL)")
		}
		url = s.URL()
		stopServer = func() { _ = s.Stop() }
	}
	a, err := nats.Connect(url)
	if err != nil {
		if stopServer != nil {
			stopServer()
		}
		t.Fatal(err)
	}
	e, err := nats.Connect(url)
	if err != nil {
		_ = a.Close()
		if stopServer != nil {
			stopServer()
		}
		t.Fatal(err)
	}
	return a, e, func() {
		_ = a.Close()
		_ = e.Close()
		if stopServer != nil {
			stopServer()
		}
	}
}

func equalValue(got, want any) bool {
	switch w := want.(type) {
	case string:
		s, ok := got.(string)
		return ok && s == w
	case float64:
		f, ok := got.(float64)
		return ok && f == w
	}
	return false
}
