package nats

import (
	"context"
	"os"
	"testing"
	"time"

	sdkbus "github.com/easydev-org/easybus-sdk/bus"
	natsrun "github.com/easydev-org/easybus-sdk/natsrun"
	"github.com/easydev-org/easybus-sdk/protocol"
)

// A channel message must survive the FULL consumer path: PublishChannel -> JetStream ->
// InboxConsume -> decode -> ParseChannelSubject.
//
// Written because a real deployment published a channel event and the consumer acked it without
// ever distributing it. Every step looked right in isolation; the failure was in a step that
// silently discards: decode() returns an error for a message it dislikes, Next() reports that the
// same way it reports "nothing to read", and the consume loop continues. Nothing is logged.
func TestChannelRoundTripThroughConsumer(t *testing.T) {
	url := os.Getenv("EASYBUS_NATS_URL")
	if url == "" {
		st, err := natsrun.Start(natsrun.Config{Storage: natsrun.Memory})
		if err != nil {
			t.Skipf("no nats-server: %v", err)
		}
		t.Cleanup(func() { _ = st.Stop() })
		url = st.URL()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pub, err := Connect(url)
	if err != nil {
		t.Fatalf("connect pub: %v", err)
	}
	defer pub.Close()
	con, err := Connect(url)
	if err != nil {
		t.Fatalf("connect con: %v", err)
	}
	defer con.Close()

	const (
		tenant  = "roundtrip-t1"
		channel = "gitea"
		topic   = "tag.agentdev/mrflow.v1.2.3"
	)

	// Durable name unique per run: a leftover consumer from an earlier run would hold the
	// messages and this one would see nothing.
	durable := "rt-" + protocol.NewID()[:8]
	sub, err := con.InboxConsume(ctx, sdkbus.InboxConsumeOpts{
		Subject: protocol.ChChannelAll,
		Durable: durable,
	})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	defer sub.Close()

	// The consumer is a pull consumer; its filter is registered asynchronously.
	time.Sleep(500 * time.Millisecond)

	if err := sdkbus.NewChannels(pub).PublishChannel(ctx, tenant, channel, topic,
		map[string]any{"repo": "agentdev/mrflow"}, "gitea"); err != nil {
		t.Fatalf("publish: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		msg, ok := sub.Next(ctx)
		if !ok {
			continue // idle pull timeout, not an error
		}
		env := msg.Envelope
		t.Logf("received: ch=%q tenant=%q", env.Ch, env.Tenant)

		// Step: the consumer derives everything from the subject.
		gotTenant, gotChannel, gotTopic, parsed := protocol.ParseChannelSubject(env.Ch)
		if !parsed {
			t.Fatalf("ParseChannelSubject(%q) failed — the consumer would ack and DROP this message", env.Ch)
		}
		if gotTenant != tenant || gotChannel != channel || gotTopic != topic {
			t.Fatalf("parsed (%q,%q,%q), want (%q,%q,%q)", gotTenant, gotChannel, gotTopic, tenant, channel, topic)
		}

		// Step: the payload the distributor forwards.
		var body struct {
			Payload map[string]any `json:"payload"`
			Source  string         `json:"source"`
		}
		if !protocol.Coerce(env.Payload, &body) {
			t.Fatalf("payload did not decode: %+v", env.Payload)
		}
		if body.Source != "gitea" {
			t.Fatalf("source = %q, want gitea", body.Source)
		}
		msg.Ack()
		return
	}
	t.Fatal("no channel message arrived within 15s")
}
