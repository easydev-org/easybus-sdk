package nats

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/easydev-org/easybus-sdk/agent"
	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/extension"
	"github.com/easydev-org/easybus-sdk/natsrun"
	"github.com/easydev-org/easybus-sdk/protocol"
)

// multiReplicaBroker starts an ephemeral broker (or uses EASYBUS_NATS_URL) and
// returns its URL plus a cleanup.
func multiReplicaBroker(t *testing.T) (string, func()) {
	t.Helper()
	if url := os.Getenv("EASYBUS_NATS_URL"); url != "" {
		return url, func() {}
	}
	s, err := natsrun.Start(natsrun.Config{Storage: natsrun.Memory})
	if err != nil {
		t.Skipf("no nats-server: %v", err)
	}
	return s.URL(), func() { _ = s.Stop() }
}

// TestDiscoveryQueueGroupSingleReply pins P0-1: with N replicas of the same
// extension subscribed to easybus.discover under queue group <extId>, a broadcast
// request is answered exactly ONCE (NATS queue-group semantics) rather than N
// times. requestMany still terminates cleanly on the single reply.
func TestDiscoveryQueueGroupSingleReply(t *testing.T) {
	url, cleanup := multiReplicaBroker(t)
	defer cleanup()
	ctx := context.Background()

	const replicas = 3
	var buses []*Bus
	var exts []*extension.Extension
	for i := 0; i < replicas; i++ {
		b, err := Connect(url)
		if err != nil {
			t.Fatal(err)
		}
		buses = append(buses, b)
		ext := extension.New(b, extension.Config{
			ID:        "multi-ext",
			Version:   "1.0",
			ReplicaID: "r" + string(rune('a'+i)),
		})
		if err := ext.Serve(ctx); err != nil {
			t.Fatal(err)
		}
		exts = append(exts, ext)
	}
	defer func() {
		for _, e := range exts {
			_ = e.Close()
		}
		for _, b := range buses {
			_ = b.Close()
		}
	}()
	time.Sleep(300 * time.Millisecond) // let queue subscriptions settle

	// A raw requestMany on easybus.discover must yield exactly one reply.
	ab, err := Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer ab.Close()
	replies, err := ab.RequestMany(ctx, protocol.ChDiscover, map[string]any{}, bus.RequestOpts{MaxWaitMs: 500, Tenant: protocol.GlobalTenant})
	if err != nil {
		t.Fatal(err)
	}
	if len(replies) != 1 {
		t.Fatalf("easybus.discover with %d replicas returned %d replies, want 1", replicas, len(replies))
	}
}

// TestPresencePerReplicaAndLiveness pins P0-2 end to end: each replica owns a
// distinct presence key, killing one keeps the extension alive, and the agent
// drops it only after the last replica key is gone.
func TestPresencePerReplicaAndLiveness(t *testing.T) {
	url, cleanup := multiReplicaBroker(t)
	defer cleanup()
	ctx := context.Background()

	extBus, err := Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer extBus.Close()
	agentBus, err := Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer agentBus.Close()

	r1 := extension.New(extBus, extension.Config{ID: "multi-ext", Version: "1.0", ReplicaID: "pod-a"})
	if err := r1.Serve(ctx); err != nil {
		t.Fatal(err)
	}
	r2 := extension.New(agentBus, extension.Config{ID: "multi-ext", Version: "1.0", ReplicaID: "pod-b"})
	if err := r2.Serve(ctx); err != nil {
		t.Fatal(err)
	}
	defer r2.Close()

	a := agent.New(agentBus)
	waitFor(t, 10*time.Second, func() bool {
		v1, _ := agentBus.KVGet(ctx, extension.PresenceBucket, "multi-ext.pod-a")
		v2, _ := agentBus.KVGet(ctx, extension.PresenceBucket, "multi-ext.pod-b")
		return v1 != "" && v2 != ""
	}, "both replica presence keys present")

	ms, err := a.Discover(ctx, 500)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, m := range ms {
		if m.Id == "multi-ext" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("Discover returned %d manifests for multi-ext, want 1", n)
	}

	// Kill replica A: its key goes, the sibling key stays, extension stays up.
	if err := r1.Close(); err != nil {
		t.Fatal(err)
	}
	if v1, _ := agentBus.KVGet(ctx, extension.PresenceBucket, "multi-ext.pod-a"); v1 != "" {
		t.Fatalf("closed replica key still present: %q", v1)
	}
	if v2, _ := agentBus.KVGet(ctx, extension.PresenceBucket, "multi-ext.pod-b"); v2 == "" {
		t.Fatal("sibling replica key was evicted")
	}
	if a.Manifest("multi-ext") == nil {
		t.Fatal("extension dropped while a sibling replica was alive")
	}

	// Last replica key gone -> offline.
	if err := agentBus.KVDelete(ctx, extension.PresenceBucket, "multi-ext.pod-b"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return a.Manifest("multi-ext") == nil },
		"extension dropped after last replica key expired")
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}
