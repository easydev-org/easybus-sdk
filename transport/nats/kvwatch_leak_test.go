package nats

import (
	"context"
	"testing"
	"time"

	natsc "github.com/nats-io/nats.go"
)

// Starting and stopping a KV watch must not leave a consumer behind.
//
// `kv.Watch` is implemented with an ORDERED consumer: a generated name (oc_*), ack_policy none,
// and a `_INBOX.*` deliver subject. Stopping the local iterator does not delete that consumer —
// the inbox subscription can keep it "active" so the server never reclaims it either. A deployment
// ended up with 3574 of them and 3581 dangling inbox subscriptions on one connection.
//
// The pattern here is the damaging one: a watch created PER REQUEST (a UI resubscribing on
// reconnect, a session view opening and closing) instead of once per process.
func TestKvWatchLeavesNoConsumerBehind(t *testing.T) {
	url := testBrokerURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	b, err := Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer b.Close()

	const bucket = "leakcheck"
	if err := b.KVPut(ctx, bucket, "k1", "v1", 0); err != nil {
		t.Fatalf("seed kv: %v", err)
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

	before := kvConsumerNames(t, js, bucket)
	t.Logf("consumers before: %d", len(before))

	// Ten short-lived watches: the shape a per-request watch takes.
	for i := 0; i < 10; i++ {
		wctx, wcancel := context.WithCancel(ctx)
		_, stop, werr := b.KvWatch(wctx, bucket, ">")
		if werr != nil {
			t.Fatalf("watch %d: %v", i, werr)
		}
		time.Sleep(80 * time.Millisecond) // let the snapshot start
		stop()
		wcancel()
	}

	var after []string
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		after = kvConsumerNames(t, js, bucket)
		if len(after) <= len(before) {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if len(after) > len(before) {
		t.Fatalf("KvWatch leaked %d of 10 consumers (before=%v after=%v)\n"+
			"a watch per request becomes a consumer per request, and each one holds a _INBOX.* "+
			"subscription that keeps it alive", len(after)-len(before), before, after)
	}
}

// kvConsumerNames lists the consumers of the stream backing a KV bucket.
func kvConsumerNames(t *testing.T, js natsc.JetStreamContext, bucket string) []string {
	t.Helper()
	stream := "KV_" + bucket
	var out []string
	for ci := range js.Consumers(stream) {
		if ci == nil {
			continue
		}
		out = append(out, ci.Name)
	}
	return out
}
