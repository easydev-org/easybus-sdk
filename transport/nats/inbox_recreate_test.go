package nats

import (
	"context"
	"testing"
	"time"

	sdkbus "github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/protocol"
)

// A pull consumer whose consumer is deleted must REBUILD, not spin.
//
// Observed in a deployment: the mailbox consumer disappeared from the stream, `Fetch` failed
// instantly with "consumer not found", the caller's idle branch retried immediately, and the loop
// ran millions of times per minute while delivering nothing. Conflating "error" with "no message"
// is what made it invisible — both returned the same value.
func TestInboxConsumeRecreatesDeletedConsumer(t *testing.T) {
	url := testBrokerURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	b, err := Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer b.Close()

	const subject = "easybus.t1.mailbox.test-recreate"
	durable := "recreate-" + protocol.NewID()[:8]

	sub, err := b.InboxConsume(ctx, sdkbus.InboxConsumeOpts{Subject: subject, Durable: durable})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	defer sub.Close()
	time.Sleep(300 * time.Millisecond)

	// Delete the consumer out from under the subscription, the way an operator or a stream reset
	// would.
	if err := b.js.DeleteConsumer("EASYBUS_MAILBOX", durable); err != nil {
		t.Skipf("cannot delete the consumer here: %v", err)
	}

	// One Next() must notice, log, and rebuild — not return a bare "nothing".
	if msg, ok := sub.Next(ctx); ok || msg != nil {
		t.Fatalf("Next() reported a message after the consumer was deleted")
	}

	// After the recreate, publishing must deliver again. This is the part that matters: a
	// consumer that merely stops spinning is still broken.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if err := b.InboxPublish(ctx, subject, map[string]any{"k": "v"},
			sdkbus.InboxPublishOpts{ID: protocol.NewID(), SessionName: "t1", Tenant: "t1"}); err != nil {
			t.Fatalf("publish: %v", err)
		}
		got, ok := sub.Next(ctx)
		if ok && got != nil {
			got.Ack()
			return // the rebuilt consumer is delivering again
		}
	}
	t.Fatal("the consumer never recovered: it stopped spinning but still delivers nothing")
}
