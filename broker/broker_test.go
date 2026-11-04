package broker_test

import (
	"testing"

	"github.com/manenim/event-queue/broker"
)

func TestPublishAssignsSequentialOffsets(t *testing.T) {
	b := broker.New[string]()

	for i, want := range []string{"a", "b", "c"} {
		msg := b.Publish(want)
		if msg.Offset != i {
			t.Fatalf("offset = %d, want %d", msg.Offset, i)
		}
		if msg.Payload != want {
			t.Fatalf("payload = %q, want %q", msg.Payload, want)
		}
	}
	if got := b.Len(); got != 3 {
		t.Fatalf("Len() = %d, want 3", got)
	}
}

func TestPublishBatchAppendsInOrder(t *testing.T) {
	b := broker.New[string]()
	b.Publish("first")

	msgs := b.PublishBatch("x", "y", "z")

	if len(msgs) != 3 {
		t.Fatalf("got %d messages, want 3", len(msgs))
	}
	for i, m := range msgs {
		if m.Offset != i+1 {
			t.Fatalf("msgs[%d].Offset = %d, want %d", i, m.Offset, i+1)
		}
	}
}
