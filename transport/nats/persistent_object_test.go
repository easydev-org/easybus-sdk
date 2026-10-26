package nats

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Persistent file bytes must have exactly ONE backend, and without it the call must FAIL.
//
// The old code fell back to the NATS object store when no durable backend was configured, which
// is not a degradation but a silent change of meaning: the bytes were still stored (so nothing
// errored), in a place the operator did not choose, under the broker's retention rules instead of
// S3's durability. Two deployments running identical code answered "where is this file"
// differently, and only the config said which.
//
// Refusing is the only answer that gives "persistent" one meaning.
func TestObjectPutPersistentRefusesWithoutS3(t *testing.T) {
	url := testBrokerURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// No Options.DurableObjects: exactly the shape of a deployment that forgot to configure S3.
	b, err := Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer b.Close()

	err = b.ObjectPutPersistent(ctx, "file-1", []byte("data"))
	if err == nil {
		t.Fatal("a persistent write with no S3 backend succeeded; the bytes went somewhere " +
			"the operator did not choose")
	}
	if !errors.Is(err, ErrNoDurableObjectStore) {
		t.Fatalf("got %v, want ErrNoDurableObjectStore so the caller can name the cause", err)
	}
}

func TestObjectGetPersistentRefusesWithoutS3(t *testing.T) {
	url := testBrokerURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	b, err := Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer b.Close()

	data, err := b.ObjectGetPersistent(ctx, "file-1")
	if err == nil {
		t.Fatalf("a persistent read with no S3 backend returned (%q, nil); a miss and 'no "+
			"backend configured' are different facts", string(data))
	}
	if !errors.Is(err, ErrNoDurableObjectStore) {
		t.Fatalf("got %v, want ErrNoDurableObjectStore", err)
	}
}

// The TRANSIENT path must keep working without S3: it is in-flight hand-off, not storage.
//
// If this ever starts requiring S3, every screenshot and PDF tool breaks in a deployment that has
// no bucket — and those bytes are explicitly TTL-bounded, so the broker is the correct home.
func TestTransientObjectWorksWithoutS3(t *testing.T) {
	url := testBrokerURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	b, err := Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer b.Close()

	if err := b.ObjectPut(ctx, "transient-1", []byte("chunk")); err != nil {
		t.Fatalf("transient put failed without S3: %v", err)
	}
	got, err := b.ObjectGet(ctx, "transient-1")
	if err != nil {
		t.Fatalf("transient get: %v", err)
	}
	if string(got) != "chunk" {
		t.Fatalf("got %q, want %q", string(got), "chunk")
	}
}
