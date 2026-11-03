package nats

import (
	"encoding/json"
	"testing"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	easybus "github.com/easydev-org/easybus-sdk"
	easybusv1 "github.com/easydev-org/easybus-sdk/gen/easybus/v1"
)

/**
 * THE WIRE IS PROTOBUF, AND THE BODY SURVIVES IT.
 *
 * These exist because a bug lived here for a long time with nothing watching: the envelope was
 * hand-assembled as a `map[string]any` and JSON-marshalled, while the .proto declared the same shape
 * separately. A `GO STRUCT` handed to `structpb.NewValue` is refused, the refusal was folded to null,
 * and a prompt body arrived EMPTY — delivered, acked, and carrying nothing, which from the outside
 * looks like a model that ignored the message.
 */

// A struct body must arrive as the object it marshals to, not as null.
//
// THIS IS THE BUG. `agent.WakePayload` is a struct; `structpb.NewValue` refuses structs; the refusal
// used to become NullValue. Every user prompt took this path.
func TestStructPayloadSurvivesTheWire(t *testing.T) {
	type wake struct {
		Type      string `json:"type"`
		MessageID string `json:"message_id"`
		Text      string `json:"text"`
	}
	body := wake{Type: "trigger", MessageID: "m-1", Text: "hello"}

	data, err := buildEnvelope("queue", "easybus.t.mailbox.s", body, "t", "id-1", "s", "")
	if err != nil {
		t.Fatalf("buildEnvelope: %v", err)
	}
	env := &easybusv1.Envelope{}
	if err := proto.Unmarshal(data, env); err != nil {
		t.Fatalf("the envelope is not valid protobuf: %v", err)
	}
	if env.Payload == nil {
		t.Fatal("the payload is nil: a struct body was folded away")
	}
	got, ok := env.Payload.AsInterface().(map[string]any)
	if !ok {
		t.Fatalf("payload is %T, want an object", env.Payload.AsInterface())
	}
	// THE TEXT IS THE POINT. An object with the right keys but an empty body is exactly the failure.
	if got["text"] != "hello" {
		t.Errorf("text = %v, want hello (the whole payload was %v)", got["text"], got)
	}
	if got["message_id"] != "m-1" {
		t.Errorf("message_id = %v, want m-1", got["message_id"])
	}
}

// A map body — what extensions send — is unaffected.
func TestMapPayloadSurvivesTheWire(t *testing.T) {
	data, err := buildEnvelope("queue", "easybus.t.mailbox.s",
		map[string]any{"k": "v"}, "t", "id", "s", "")
	if err != nil {
		t.Fatal(err)
	}
	env := &easybusv1.Envelope{}
	if err := proto.Unmarshal(data, env); err != nil {
		t.Fatal(err)
	}
	got, _ := env.Payload.AsInterface().(map[string]any)
	if got["k"] != "v" {
		t.Fatalf("payload = %v, want {k: v}", got)
	}
}

// The fields the transport relies on must round-trip, including the ones it treats as optional.
func TestEnvelopeFieldsRoundTrip(t *testing.T) {
	data, err := buildEnvelope("queue", "easybus.t.mailbox.s",
		map[string]any{"x": 1}, "t", "id-9", "sess", "reply.inbox")
	if err != nil {
		t.Fatal(err)
	}
	env := &easybusv1.Envelope{}
	if err := proto.Unmarshal(data, env); err != nil {
		t.Fatal(err)
	}
	if env.GetV() != 2 {
		t.Errorf("v = %d, want 2", env.GetV())
	}
	if env.Tenant != "t" || env.Ch != "easybus.t.mailbox.s" {
		t.Errorf("tenant/ch = %q/%q", env.Tenant, env.Ch)
	}
	if env.GetKind() != easybusv1.Envelope_KIND_QUEUE {
		t.Errorf("kind = %v, want QUEUE", env.GetKind())
	}
	if env.GetId() != "id-9" || env.GetSessionName() != "sess" || env.GetReplyTo() != "reply.inbox" {
		t.Errorf("id/session/reply = %q/%q/%q", env.GetId(), env.GetSessionName(), env.GetReplyTo())
	}
}

// An unset optional field must stay UNSET, so a consumer can tell "absent" from "empty string".
func TestOptionalFieldsStayAbsent(t *testing.T) {
	data, err := buildEnvelope("pub", "easybus.t.ch", map[string]any{}, "t", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	env := &easybusv1.Envelope{}
	if err := proto.Unmarshal(data, env); err != nil {
		t.Fatal(err)
	}
	if env.Id != nil {
		t.Errorf("id is set to %q; an empty id must be absent", env.GetId())
	}
	if env.SessionName != nil {
		t.Errorf("session_name is set; it must be absent")
	}
}

// decode enforces the tenant/subject agreement, which is what stops a message being read as another
// tenant's.
func TestDecodeRejectsATenantMismatch(t *testing.T) {
	data, _ := buildEnvelope("pub", "easybus.alice.ch", map[string]any{}, "bob", "", "", "")
	m := &nats.Msg{Subject: "easybus.alice.ch", Data: data}
	if _, err := decode(m); err == nil {
		t.Fatal("a mismatched tenant was accepted")
	}
}

// AN OLD JSON ENVELOPE MUST NOT BE SILENTLY ACCEPTED. The wire changed to protobuf, and a v2 JSON body
// bytes happen to start with '{', which proto.Unmarshal rejects — this pins that it is rejected rather
// than read as an empty envelope.
func TestDecodeRejectsALegacyJSONEnvelope(t *testing.T) {
	legacy, _ := json.Marshal(map[string]any{
		"v": 2, "ch": "easybus.t.ch", "tenant": "t", "kind": "pub", "payload": map[string]any{"a": 1},
	})
	m := &nats.Msg{Subject: "easybus.t.ch", Data: legacy}
	if env, err := decode(m); err == nil {
		t.Fatalf("a legacy JSON envelope decoded as %v; it must be rejected", env)
	}
}

/**
 * THE SIGNATURE MUST BE OVER THE SAME KIND WORD THE HEADER IS.
 *
 * The header is built from the TRANSPORT word ("queue"), and verification reads the kind back out of the
 * envelope — where it is a protobuf ENUM. `GetKind().String()` returns "KIND_QUEUE", so signing and
 * verifying disagreed by a prefix and every signed message failed against its own signature. Nothing
 * caught it because the identity test needs a live broker and skips without one.
 */
func TestSignatureKindAgreesWithTheEnvelope(t *testing.T) {
	for _, kind := range []string{"req", "pub", "queue"} {
		data, err := buildEnvelope(kind, "easybus.t.ch", map[string]any{"a": 1}, "t", "id", "", "")
		if err != nil {
			t.Fatal(err)
		}
		env := &easybusv1.Envelope{}
		if err := proto.Unmarshal(data, env); err != nil {
			t.Fatal(err)
		}
		// What verifyMsg now derives, and what the sender signed with.
		if got := easybus.EnvelopeKindString(env.GetKind()); got != kind {
			t.Errorf("%q round-tripped as %q; the signature would not verify", kind, got)
		}
	}
}
