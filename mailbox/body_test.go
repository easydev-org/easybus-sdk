package mailbox

import "testing"

/**
 * THREE PRODUCERS, ONE SHAPE.
 *
 * The queue accepted these and every consumer decoded them itself with its own guess about field
 * names — which is where `message_id` versus `messageId` went wrong. BodyOf is now the only place
 * that knows, so each accepted shape is pinned here.
 */
func TestBodyOfAcceptsEveryProducerShape(t *testing.T) {
	t.Run("struct", func(t *testing.T) {
		b, err := BodyOf(Body{MessageID: "m1", Text: "hi"})
		if err != nil || b.MessageID != "m1" || b.Text != "hi" {
			t.Fatalf("body = %+v, err = %v", b, err)
		}
	})

	t.Run("map-from-json", func(t *testing.T) {
		b, err := BodyOf(map[string]any{"message_id": "m2", "text": "hello", "channel": "gitdev"})
		if err != nil || b.MessageID != "m2" || b.Text != "hello" || b.Channel != "gitdev" {
			t.Fatalf("body = %+v, err = %v", b, err)
		}
	})

	// A JSON STRING is what the row-based queue stored, and a producer that sends one is using the
	// shape that storage required rather than making a mistake.
	t.Run("json-string", func(t *testing.T) {
		b, err := BodyOf(`{"message_id":"m3","text":"from a string"}`)
		if err != nil || b.MessageID != "m3" || b.Text != "from a string" {
			t.Fatalf("body = %+v, err = %v", b, err)
		}
	})

	t.Run("nil-is-empty-not-an-error", func(t *testing.T) {
		b, err := BodyOf(nil)
		if err != nil || b.Text != "" {
			t.Fatalf("body = %+v, err = %v", b, err)
		}
	})

	// Unknown keys are surface, not error: an extension may send fields the agent has never heard
	// of, and rejecting those would throw away the text too.
	t.Run("unknown-fields-are-ignored", func(t *testing.T) {
		b, err := BodyOf(map[string]any{"text": "keep me", "future_field": 42})
		if err != nil || b.Text != "keep me" {
			t.Fatalf("body = %+v, err = %v", b, err)
		}
	})

	// A BARE STRING BECOMES TEXT. It used to be a decode error, which dropped the whole message —
	// including the only thing it said.
	t.Run("plain-string-becomes-text", func(t *testing.T) {
		b, err := BodyOf("just-a-string")
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if b.Text != "just-a-string" {
			t.Fatalf("body = %+v, want the string as text", b)
		}
	})

	// A number or a list has no text and no fields; it must not be an error either, because the
	// producer sent a valid payload in a shape this queue has nothing to say about.
	t.Run("non-object-without-text-is-empty", func(t *testing.T) {
		if b, err := BodyOf(42); err != nil || b.Text != "" {
			t.Fatalf("body = %+v, err = %v", b, err)
		}
	})

	// Channel provenance must survive, because a tool reading the event structurally depends on it.
	t.Run("channel-provenance", func(t *testing.T) {
		b, err := BodyOf(Body{Text: "a release", Channel: "gitdev", Topic: "release.o/r.v1", Data: "x"})
		if err != nil || b.Channel != "gitdev" || b.Topic != "release.o/r.v1" || b.Data != "x" {
			t.Fatalf("body = %+v, err = %v", b, err)
		}
	})
}
