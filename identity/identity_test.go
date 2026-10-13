package identity

import "testing"

func TestSignAndVerify(t *testing.T) {
	id := Identity{ID: "agent-1", Secret: "s3cret"}
	f := Fields{Ch: "easybus.discover", Kind: "req", ID: "c1", Payload: map[string]any{"x": 1}}

	h := AuthHeader(id, f)
	if h.ID != "agent-1" {
		t.Fatalf("id = %q", h.ID)
	}
	if !Verify("agent-1", "s3cret", f, h.Sig) {
		t.Fatal("verify should pass")
	}

	// wrong secret fails
	if Verify("agent-1", "wrong", f, h.Sig) {
		t.Fatal("wrong secret should fail")
	}
	// wrong field fails (tampered payload)
	f2 := f
	f2.Payload = map[string]any{"x": 2}
	if Verify("agent-1", "s3cret", f2, h.Sig) {
		t.Fatal("tampered payload should fail")
	}
}
