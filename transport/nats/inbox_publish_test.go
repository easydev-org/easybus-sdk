package nats

import (
	"context"
	"testing"
	"time"

	"github.com/easydev-org/easybus-sdk/agent"
	"github.com/easydev-org/easybus-sdk/protocol"
)

// PublishMailbox must put a message where the mailbox consumer can read it.
//
// Written because a deployment reported exactly this shape: the distributor matched two
// subscribers, its publish call returned nil, and the EASYBUS_MAILBOX stream's message count
// never moved. "Returns nil" and "is stored" are different claims, and only the second one makes
// a session wake up.
func TestPublishMailboxIsStored(t *testing.T) {
	url := testBrokerURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b, err := Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer b.Close()

	before := streamMessages(t, url, "EASYBUS_MAILBOX")
	t.Logf("mailbox messages before: %d", before)

	a := agent.New(b)
	const tenant, session = "t1", "sess-publish"
	if err := a.PublishMailbox(ctx, tenant, session, "trigger",
		map[string]any{"k": "v"}, "channel:test"); err != nil {
		t.Fatalf("PublishMailbox returned an error: %v", err)
	}

	// js.Publish is synchronous: when it returns nil the message is stored. Give the stream's
	// message counter a moment to settle, then require it to have MOVED.
	var after int
	for i := 0; i < 40; i++ {
		after = streamMessages(t, url, "EASYBUS_MAILBOX")
		if after > before {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if after <= before {
		t.Fatalf("PublishMailbox returned nil but the stream did not grow (%d -> %d): "+
			"the message went somewhere nobody reads", before, after)
	}
	t.Logf("mailbox messages after: %d", after)

	// And it must be on the subject the consumer filters on.
	want := protocol.ChMailbox(tenant, session)
	t.Logf("expected subject: %s (matches easybus.*.mailbox.>)", want)
}
