package nats

import (
	"os"
	"testing"

	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/conformance"
	"github.com/easydev-org/easybus-sdk/natsrun"
)

// TestConformance runs the shared suite on its OWN ephemeral broker per
// subtest (deterministic isolation). EASYBUS_NATS_URL opts into a shared
// external broker instead — state there (streams/buckets) persists across
// runs, which kv/config cases must tolerate.
func TestConformance(t *testing.T) {
	// One ephemeral broker PER SUBTEST (deterministic isolation: queue-group
	// and durable consumers otherwise leak across subtests during async
	// teardown and steal each other's deliveries). EASYBUS_NATS_URL opts into a
	// shared external broker instead.
	extURL := os.Getenv("EASYBUS_NATS_URL")
	conformance.Run(t, func(t *testing.T) (bus.Bus, bus.Bus, func()) {
		var stopServer func()
		url := extURL
		if url == "" {
			s, err := natsrun.Start(natsrun.Config{Storage: natsrun.Memory})
			if err != nil {
				t.Skipf("no nats-server: %v", err)
			}
			url = s.URL()
			stopServer = func() { _ = s.Stop() }
		}
		a, err := Connect(url)
		if err != nil {
			t.Fatal(err)
		}
		b, err := Connect(url)
		if err != nil {
			t.Fatal(err)
		}
		return a, b, func() {
			_ = a.Close()
			_ = b.Close()
			if stopServer != nil {
				stopServer()
			}
		}
	})
}
