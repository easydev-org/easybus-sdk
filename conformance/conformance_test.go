package conformance

import (
	"os"
	"testing"

	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/natsrun"
	"github.com/easydev-org/easybus-sdk/transport/nats"
)

// TestConformance runs the full protocol suite. Every subtest gets its OWN
// nats-server (memory) plus two fresh connections (agent + extension roles):
// a shared broker leaks queue-group subscriptions across subtests during
// teardown (unsubscribe propagation is asynchronous), which lets a dying
// extension steal the next subtest's deliveries — flaky under -race and
// on loaded hosts. A per-subtest broker makes isolation deterministic.
//
// When EASYBUS_NATS_URL is set the suite instead runs against that external
// broker (shared), which is the opt-in mode for live-cluster checks.
func TestConformance(t *testing.T) {
	// One ephemeral broker PER SUBTEST: durable/queue consumers leak state
	// across subtests on a shared broker during async teardown, stealing
	// each other's deliveries (poison/DLQ/config recovery cases especially).
	// EASYBUS_NATS_URL opts into a shared external broker for live checks.
	extURL := os.Getenv("EASYBUS_NATS_URL")
	Run(t, func(t *testing.T) (bus.Bus, bus.Bus, func()) {
		var stopServer func()
		url := extURL
		if url == "" {
			s, err := natsrun.Start(natsrun.Config{Storage: natsrun.Memory})
			if err != nil {
				t.Skip("no nats-server available (install nats-server or set EASYBUS_NATS_SERVER_BIN or EASYBUS_NATS_URL)")
			}
			url = s.URL()
			stopServer = func() { _ = s.Stop() }
		}
		a, err := nats.Connect(url)
		if err != nil {
			t.Fatal(err)
		}
		e, err := nats.Connect(url)
		if err != nil {
			t.Fatal(err)
		}
		return a, e, func() {
			_ = a.Close()
			_ = e.Close()
			if stopServer != nil {
				stopServer()
			}
		}
	})
}
