package nats

import (
	"testing"

	"github.com/easydev-org/easybus-sdk/identity"
	"github.com/easydev-org/easybus-sdk/protocol"
	"github.com/nats-io/nats.go"
)

type signProbeBody struct {
	Text string `json:"text"`
	N    int    `json:"n"`
}

// A MESSAGE MUST VERIFY AGAINST ITS OWN SIGNATURE.
//
// It did not. Signing covered the CALLER'S Go value; verification covered the payload read back out
// of the structpb, and those are two different JSON renderings:
//
//	{"text":"hi","n":1}  signed
//	{"n":1,"text":"hi"}  verified      (structpb orders keys)
//
// A struct payload therefore failed in the same process, with the same value, and nothing tested it
// because the identity option is off by default.
func TestMessageVerifiesAgainstItsOwnSignature(t *testing.T) {
	b := &Bus{idn: &identity.Identity{ID: "me", Secret: "s3cret"}}
	ch := protocol.ChMailbox("t", "s")
	cases := []struct {
		name    string
		payload any
	}{
		{"map", map[string]any{"text": "hi", "n": 1}},
		{"struct", signProbeBody{Text: "hi", N: 1}},
		{"struct-pointer", &signProbeBody{Text: "hi", N: 1}},
		{"nil", nil},
		{"nested", map[string]any{"a": map[string]any{"b": []any{1.0, 2.0}}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			data, err := buildEnvelope("queue", ch, c.payload, "t", "id1", "s", "")
			if err != nil {
				t.Fatalf("build envelope: %v", err)
			}
			m := nats.NewMsg(ch)
			m.Data = data
			b.signMsg(m)
			if m.Header.Get("easybus-sig") == "" {
				t.Fatal("signMsg produced no signature")
			}
			if !b.verifyMsg(m) {
				t.Fatal("the signature does not verify against the message that was signed")
			}
		})
	}
}

// Verification must REJECT, not accept, when the body is altered after signing.
func TestTamperedPayloadIsRejected(t *testing.T) {
	b := &Bus{idn: &identity.Identity{ID: "me", Secret: "s3cret"}}
	ch := protocol.ChMailbox("t", "s")
	data, err := buildEnvelope("queue", ch, map[string]any{"text": "hi"}, "t", "id1", "s", "")
	if err != nil {
		t.Fatal(err)
	}
	m := nats.NewMsg(ch)
	m.Data = data
	b.signMsg(m)

	tampered, err := buildEnvelope("queue", ch, map[string]any{"text": "bye"}, "t", "id1", "s", "")
	if err != nil {
		t.Fatal(err)
	}
	m2 := nats.NewMsg(ch)
	m2.Data = tampered
	m2.Header = m.Header
	if b.verifyMsg(m2) {
		t.Fatal("a tampered payload verified")
	}
}

// A message signed for one subject must not verify on another.
func TestSignatureIsBoundToTheSubject(t *testing.T) {
	b := &Bus{idn: &identity.Identity{ID: "me", Secret: "s3cret"}}
	ch := protocol.ChMailbox("t", "s")
	data, err := buildEnvelope("queue", ch, map[string]any{"text": "hi"}, "t", "id1", "s", "")
	if err != nil {
		t.Fatal(err)
	}
	m := nats.NewMsg(ch)
	m.Data = data
	b.signMsg(m)

	moved := nats.NewMsg(protocol.ChMailbox("t", "other"))
	moved.Data = data
	moved.Header = m.Header
	if b.verifyMsg(moved) {
		t.Fatal("a signature verified on a different subject")
	}
}

// Missing headers are a rejection, not a pass.
func TestUnsignedMessageIsRejected(t *testing.T) {
	b := &Bus{idn: &identity.Identity{ID: "me", Secret: "s3cret"}}
	data, err := buildEnvelope("queue", protocol.ChMailbox("t", "s"), map[string]any{"text": "hi"}, "t", "id1", "s", "")
	if err != nil {
		t.Fatal(err)
	}
	m := nats.NewMsg(protocol.ChMailbox("t", "s"))
	m.Data = data
	if b.verifyMsg(m) {
		t.Fatal("an unsigned message verified")
	}
	// With no identity configured, verification is off by design.
	openBus := &Bus{}
	if !openBus.verifyMsg(m) {
		t.Fatal("verification must be a no-op when no identity is configured")
	}
}
