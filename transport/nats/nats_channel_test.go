package nats

import (
	"context"
	"os"
	"testing"
	"time"

	sdkbus "github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/natsrun"
)

// A channel event must survive the trip: published through JetStream, delivered to a
// consumer.
//
// This test exists because the first implementation of exactly this path LOOKED right and
// delivered nothing, twice over:
//
//   - no stream claimed `channel.>`, so js.Publish retained the message and nobody read it;
//   - the consumer used a core NATS subscription, which never sees JetStream-published
//     messages at all.
//
// Neither failure produced an error anywhere. Only asserting on delivery catches it.
func TestChannelPublishAndConsume(t *testing.T) {
	url := os.Getenv("EASYBUS_NATS_URL")
	if url == "" {
		s, err := natsrun.Start(natsrun.Config{Storage: natsrun.Memory})
		if err != nil {
			t.Skipf("no nats-server: %v", err)
		}
		t.Cleanup(func() { _ = s.Stop() })
		url = s.URL()
	}
	b, err := Connect(url)
	if err != nil {
		t.Skipf("nats unavailable: %v", err)
	}
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	ch := sdkbus.NewChannels(b)
	got := make(chan sdkbus.ChannelMessage, 4)
	stop, err := ch.ConsumeChannels(ctx, "chan-test", func(m sdkbus.ChannelMessage) error {
		got <- m
		return nil
	})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	defer stop()
	// Let the consumer exist before publishing, so this test is about the path and not about
	// a race with consumer creation.
	time.Sleep(750 * time.Millisecond)

	if err := ch.PublishChannel(ctx, "alice", "gitea", "tag.agentdev/mrflow.v1.2.3",
		map[string]any{"repo": "agentdev/mrflow"}, "gitea"); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case m := <-got:
		if m.Tenant != "alice" || m.Channel != "gitea" || m.Topic != "tag.agentdev/mrflow.v1.2.3" {
			t.Fatalf("delivered %+v", m)
		}
		if m.Source != "gitea" {
			t.Errorf("source = %q", m.Source)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("channel event was published but never consumed")
	}
}

// A message published while NO consumer is running must still arrive once one starts.
//
// That is what the durable consumer buys: an agent restart does not drop the events that
// happened during it.
func TestChannelSurvivesConsumerRestart(t *testing.T) {
	url := os.Getenv("EASYBUS_NATS_URL")
	if url == "" {
		s, err := natsrun.Start(natsrun.Config{Storage: natsrun.Memory})
		if err != nil {
			t.Skipf("no nats-server: %v", err)
		}
		t.Cleanup(func() { _ = s.Stop() })
		url = s.URL()
	}
	b, err := Connect(url)
	if err != nil {
		t.Skipf("nats unavailable: %v", err)
	}
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	ch := sdkbus.NewChannels(b)

	// Publish FIRST, with no consumer at all.
	if err := ch.PublishChannel(ctx, "alice", "gitea", "tag.v-while-down",
		map[string]any{"tag": "v-while-down"}, "gitea"); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Now start consuming; the retained message must be delivered.
	got := make(chan sdkbus.ChannelMessage, 8)
	stop, err := ch.ConsumeChannels(ctx, "chan-restart", func(m sdkbus.ChannelMessage) error {
		got <- m
		return nil
	})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	defer stop()

	deadline := time.After(10 * time.Second)
	for {
		select {
		case m := <-got:
			if m.Topic == "tag.v-while-down" {
				return // found it
			}
		case <-deadline:
			t.Fatal("the event published before the consumer started was lost")
		}
	}
}

// Publishers cannot reach another tenant, and a consumer only ever reads its own
// subscription's tenant: the subject is the routing key and the caller never sets it.
func TestChannelTenantIsolation(t *testing.T) {
	url := os.Getenv("EASYBUS_NATS_URL")
	if url == "" {
		s, err := natsrun.Start(natsrun.Config{Storage: natsrun.Memory})
		if err != nil {
			t.Skipf("no nats-server: %v", err)
		}
		t.Cleanup(func() { _ = s.Stop() })
		url = s.URL()
	}
	b, err := Connect(url)
	if err != nil {
		t.Skipf("nats unavailable: %v", err)
	}
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ch := sdkbus.NewChannels(b)

	got := make(chan sdkbus.ChannelMessage, 4)
	stop, err := ch.ConsumeChannels(ctx, "chan-tenant", func(m sdkbus.ChannelMessage) error {
		got <- m
		return nil
	})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	defer stop()
	time.Sleep(500 * time.Millisecond)

	// Same channel and topic in two tenants: the tenant must come back from the subject.
	if err := ch.PublishChannel(ctx, "bob", "gitea", "tag.same", map[string]any{}, "gitea"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case m := <-got:
		if m.Tenant != "bob" {
			t.Errorf("tenant = %q, want bob (from the subject)", m.Tenant)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("event never delivered")
	}
}
