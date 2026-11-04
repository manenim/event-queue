package broker_test

import (
	"slices"
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

// payloads extracts the payloads from msgs, in order.
func payloads[T any](msgs []broker.Message[T]) []T {
	out := make([]T, len(msgs))
	for i, m := range msgs {
		out[i] = m.Payload
	}
	return out
}

func mustRead(t *testing.T, b *broker.Broker[string], id string, max int) []string {
	t.Helper()
	msgs, err := b.Read(id, max)
	if err != nil {
		t.Fatalf("Read(%q): %v", id, err)
	}
	return payloads(msgs)
}

func TestReadReturnsMessagesInPublishOrder(t *testing.T) {
	b := broker.New[string]()
	b.Subscribe("s1")
	b.PublishBatch("1", "2", "3", "4")

	if got, want := mustRead(t, b, "s1", 0), []string{"1", "2", "3", "4"}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestLateSubscriberReadsFromStartOfLog(t *testing.T) {
	b := broker.New[string]()
	b.PublishBatch("before-1", "before-2")
	b.Subscribe("late")
	b.Publish("after-1")

	want := []string{"before-1", "before-2", "after-1"}
	if got := mustRead(t, b, "late", 0); !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestEachMessageIsConsumedAtMostOnce(t *testing.T) {
	b := broker.New[string]()
	b.Subscribe("s1")
	b.PublishBatch("a", "b")

	mustRead(t, b, "s1", 0)
	if got := mustRead(t, b, "s1", 0); len(got) != 0 {
		t.Fatalf("second read returned %v, want nothing", got)
	}

	b.Publish("c")
	if got, want := mustRead(t, b, "s1", 0), []string{"c"}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want only the new message %v", got, want)
	}
}

func TestSubscribeTwiceIsNoOp(t *testing.T) {
	b := broker.New[string]()
	if !b.Subscribe("s1") {
		t.Fatal("first Subscribe should register the subscriber")
	}
	b.PublishBatch("a", "b")
	mustRead(t, b, "s1", 0)

	if b.Subscribe("s1") {
		t.Fatal("second Subscribe should report no change")
	}
	// The repeat subscribe must not reset the read position.
	if got := mustRead(t, b, "s1", 0); len(got) != 0 {
		t.Fatalf("after re-subscribe got %v, want nothing (offset must be kept)", got)
	}
}

func TestSubscribersHaveIndependentReadPositions(t *testing.T) {
	b := broker.New[string]()
	b.Subscribe("s1")
	b.Subscribe("s2")
	b.PublishBatch("a", "b", "c")

	if got, want := mustRead(t, b, "s1", 2), []string{"a", "b"}; !slices.Equal(got, want) {
		t.Fatalf("s1 got %v, want %v", got, want)
	}
	// s1 reading must not move s2.
	if got, want := mustRead(t, b, "s2", 0), []string{"a", "b", "c"}; !slices.Equal(got, want) {
		t.Fatalf("s2 got %v, want %v", got, want)
	}
	if got, want := mustRead(t, b, "s1", 0), []string{"c"}; !slices.Equal(got, want) {
		t.Fatalf("s1 got %v, want %v", got, want)
	}
}

func TestReadRespectsMax(t *testing.T) {
	b := broker.New[string]()
	b.Subscribe("s1")
	b.PublishBatch("a", "b", "c")

	if got, want := mustRead(t, b, "s1", 1), []string{"a"}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := mustRead(t, b, "s1", 10), []string{"b", "c"}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestReadWithoutSubscribingFails(t *testing.T) {
	b := broker.New[string]()
	b.Publish("a")

	if _, err := b.Read("nobody", 0); err != broker.ErrNotSubscribed {
		t.Fatalf("err = %v, want ErrNotSubscribed", err)
	}
	if _, err := b.Pending("nobody"); err != broker.ErrNotSubscribed {
		t.Fatalf("Pending err = %v, want ErrNotSubscribed", err)
	}
}

func TestPendingCountsUnreadMessages(t *testing.T) {
	b := broker.New[string]()
	b.Subscribe("s1")
	b.PublishBatch("a", "b", "c")
	mustRead(t, b, "s1", 1)

	if n, _ := b.Pending("s1"); n != 2 {
		t.Fatalf("Pending = %d, want 2", n)
	}
}

func TestReadReturnsCopyOfLog(t *testing.T) {
	b := broker.New[string]()
	b.Subscribe("s1")
	b.Subscribe("s2")
	b.Publish("original")

	msgs, _ := b.Read("s1", 0)
	msgs[0].Payload = "tampered"

	if got := mustRead(t, b, "s2", 0); got[0] != "original" {
		t.Fatalf("s2 got %q; changing a returned slice must not change the log", got[0])
	}
}

func TestBrokerSupportsStructPayloads(t *testing.T) {
	type OrderPlaced struct {
		OrderID string
		Amount  int
	}
	b := broker.New[OrderPlaced]()
	b.Subscribe("billing")
	b.Publish(OrderPlaced{OrderID: "ord-1", Amount: 2500})

	msgs, err := b.Read("billing", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].Payload.OrderID != "ord-1" || msgs[0].Payload.Amount != 2500 {
		t.Fatalf("got %+v", msgs)
	}
}
