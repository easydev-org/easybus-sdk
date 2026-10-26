package nats

import (
	"context"
	"testing"
	"time"

	natsc "github.com/nats-io/nats.go"

	sdkbus "github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/protocol"
)

// Every ephemeral consumer a request-level call creates must be GONE when that call returns.
//
// This is the leak that filled a deployment with 3574 ordered consumers over 14 days, each with a
// dangling `_INBOX.*` subscription on the agent's connection: `Replay` opens a JetStream
// subscription per call, so a UI that reconnects to WatchSession leaks one consumer per
// reconnect — and the server never reclaims them because the client keeps the inbox subscribed.
//
// A consumer count that merely looks small proves nothing; the count has to return to its
// starting value after the work is done.
func TestReplayLeavesNoConsumerBehind(t *testing.T) {
	url := testBrokerURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b, err := Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer b.Close()

	// Publish onto a stream-backed subject so Replay has somewhere to look.
	const tenant, session = "t1", "leak-check"
	subject := protocol.ChSessionEvents(tenant, session)
	for i := 0; i < 3; i++ {
		if err := b.InboxPublish(ctx, subject, map[string]any{"i": i},
			sdkbus.InboxPublishOpts{ID: protocol.NewID(), SessionName: session, Tenant: tenant}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	nc, err := natsc.Connect(url, natsc.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	defer func() { _ = nc.Drain() }()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	before := consumerNames(t, js)

	// Do the thing that leaks: replay the subject a number of times, as repeated WatchSession
	// connects would.
	for i := 0; i < 5; i++ {
		if _, err := b.Replay(ctx, subject); err != nil {
			t.Fatalf("replay %d: %v", i, err)
		}
	}

	// Consumers are deleted asynchronously; give the server a moment, then require the count to
	// return to where it started. A consumer left behind is the whole defect.
	var after []string
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		after = consumerNames(t, js)
		if len(after) <= len(before) {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if len(after) > len(before) {
		t.Fatalf("Replay leaked %d consumers: before=%v after=%v\n"+
			"each leaked consumer also holds a _INBOX.* subscription on the client connection",
			len(after)-len(before), before, after)
	}
	if len(after) < len(before) {
		t.Logf("consumers went %d -> %d (fine: a pre-existing one was reclaimed)", len(before), len(after))
	}
}

// consumerNames lists every consumer across the streams the SDK uses.
func consumerNames(t *testing.T, js natsc.JetStreamContext) []string {
	t.Helper()
	var out []string
	for _, stream := range []string{streamEvents, streamMailbox, streamDLQ} {
		// Consumers() walks one page at a time; ConsumersInfo() returns a single page. A leak of
		// thousands of consumers is exactly the case where one page is not enough.
		for ci := range js.Consumers(stream) {
			if ci == nil {
				continue
			}
			out = append(out, stream+"/"+ci.Name)
		}
	}
	return out
}
