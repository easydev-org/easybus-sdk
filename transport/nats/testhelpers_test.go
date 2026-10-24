package nats

import (
	"os"
	"testing"

	natsc "github.com/nats-io/nats.go"

	"github.com/easydev-org/easybus-sdk/natsrun"
)

// testBrokerURL returns a broker for the test: the shared one when the environment provides it,
// otherwise a private in-process server. Skipping when neither is available keeps a missing
// binary from masquerading as a failure.
func testBrokerURL(t *testing.T) string {
	t.Helper()
	if url := os.Getenv("EASYBUS_NATS_URL"); url != "" {
		if nc, err := natsc.Connect(url, natsc.Timeout(2_000_000_000)); err == nil {
			_ = nc.Drain()
			return url
		}
	}
	s, err := natsrun.Start(natsrun.Config{Storage: natsrun.Memory})
	if err != nil {
		t.Skipf("no nats-server: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop() })
	return s.URL()
}

// streamMessages reads a stream's stored message count.
//
// The count is the only honest check for "was it stored": a publish that returns nil and a stream
// that did not grow is exactly the failure this file exists to catch.
func streamMessages(t *testing.T, url, stream string) int {
	t.Helper()
	nc, err := natsc.Connect(url, natsc.Timeout(2_000_000_000))
	if err != nil {
		t.Fatalf("connect for stream info: %v", err)
	}
	defer func() { _ = nc.Drain() }()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	info, err := js.StreamInfo(stream)
	if err != nil {
		// A stream nobody has created yet has no messages; that is 0, not an error.
		return 0
	}
	return int(info.State.Msgs)
}
