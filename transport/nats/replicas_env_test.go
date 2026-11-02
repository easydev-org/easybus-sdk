package nats

import (
	"testing"
)

// The env var is the deployment's channel for a property that describes the broker topology, not the
// extension. These pin the two things that matter: a valid value is honoured, and a bad one falls
// back instead of taking the process down.

func TestReplicasFromEnvUnset(t *testing.T) {
	t.Setenv(EnvReplicas, "")
	if got := replicasFromEnv(); got != 0 {
		t.Fatalf("unset => %d, want 0 (broker default)", got)
	}
}

func TestReplicasFromEnvValid(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want int
	}{{"1", 1}, {"3", 3}, {"5", 5}} {
		t.Setenv(EnvReplicas, tc.raw)
		if got := replicasFromEnv(); got != tc.want {
			t.Fatalf("%q => %d, want %d", tc.raw, got, tc.want)
		}
	}
}

// A BAD VALUE MUST NOT STOP THE PROCESS. The alternative — refusing to start — would take a working
// deployment down over a tunable whose only effect is how many copies a new stream gets.
func TestReplicasFromEnvInvalidFallsBack(t *testing.T) {
	for _, raw := range []string{"three", "-1", "3.5", " "} {
		t.Setenv(EnvReplicas, raw)
		if got := replicasFromEnv(); got != 0 {
			t.Fatalf("%q => %d, want 0 (fallback)", raw, got)
		}
	}
}

// Options.Replicas wins over the environment: an explicit caller knows something the deployment does
// not, and silently overriding it would make ConnectWithOptions unpredictable.
func TestExplicitReplicasWinsOverEnv(t *testing.T) {
	t.Setenv(EnvReplicas, "5")
	b, err := ConnectWithOptions("", Options{Replicas: 3})
	if err != nil {
		t.Skipf("no broker reachable: %v", err) // this test needs nothing from the broker but the call
	}
	defer b.Close()
	if b.replicas != 3 {
		t.Fatalf("explicit Options.Replicas = 3, got %d — env must not override it", b.replicas)
	}
}
