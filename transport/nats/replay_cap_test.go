package nats

import (
	"context"
	"testing"
	"time"

	sdkbus "github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/protocol"
)

// Replay must never return an unbounded window.
//
// The stream is retained for 24h, so "the whole window" means "whatever the busiest day
// produced" — allocated in one slice, once per WatchSession connect. A reconnecting UI therefore
// costs whatever yesterday cost.
func TestReplayIsCapped(t *testing.T) {
	url := testBrokerURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	b, err := Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer b.Close()

	const session = "replay-cap"
	subject := protocol.ChSessionEvents("t1", session)

	// More messages than the cap, so the cap is actually exercised.
	total := replayMaxMessages + 50
	for i := 0; i < total; i++ {
		if err := b.InboxPublish(ctx, subject, map[string]any{"i": i},
			sdkbus.InboxPublishOpts{ID: protocol.NewID(), SessionName: session, Tenant: "t1"}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	envs, err := b.Replay(ctx, subject)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(envs) > replayMaxMessages {
		t.Fatalf("Replay returned %d envelopes, cap is %d: an unbounded window is an unbounded "+
			"allocation per WatchSession connect", len(envs), replayMaxMessages)
	}
	if len(envs) < replayMaxMessages {
		t.Fatalf("Replay returned only %d of %d published; the cap must not lose the window it "+
			"keeps", len(envs), total)
	}
	t.Logf("capped at %d (published %d)", len(envs), total)
}
