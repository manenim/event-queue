package broker_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/manenim/event-queue/broker"
)

func readAll(t *testing.T, s *broker.Subscriber[string]) []string {
	t.Helper()
	msgs, err := s.ReadAll()
	if err != nil {
		t.Fatalf("%s ReadAll: %v", s.ID(), err)
	}
	return payloads(msgs)
}

func TestPublisherAndSubscriberRoundTrip(t *testing.T) {
	b := broker.New[string]()
	pub := broker.NewPublisher(b)
	sub := broker.NewSubscriber[string]("s1")
	if err := sub.Subscribe(b); err != nil {
		t.Fatal(err)
	}

	pub.Publish("one")
	pub.PublishBatch("two", "three")

	if got, want := readAll(t, sub), []string{"one", "two", "three"}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if n, _ := sub.Pending(); n != 0 {
		t.Fatalf("Pending = %d, want 0", n)
	}
}

func TestSubscriberSubscribeTwiceIsNoOp(t *testing.T) {
	b := broker.New[string]()
	sub := broker.NewSubscriber[string]("s1")
	b.Publish("a")

	if err := sub.Subscribe(b); err != nil {
		t.Fatal(err)
	}
	readAll(t, sub)

	if err := sub.Subscribe(b); err != nil {
		t.Fatalf("repeat Subscribe: %v, want nil (no-op)", err)
	}
	if got := readAll(t, sub); len(got) != 0 {
		t.Fatalf("got %v after repeat Subscribe, want nothing", got)
	}
}

func TestSubscriberReadBeforeSubscribeFails(t *testing.T) {
	sub := broker.NewSubscriber[string]("s1")
	if _, err := sub.ReadAll(); !errors.Is(err, broker.ErrNotSubscribed) {
		t.Fatalf("err = %v, want ErrNotSubscribed", err)
	}
	if _, err := sub.Pending(); !errors.Is(err, broker.ErrNotSubscribed) {
		t.Fatalf("Pending err = %v, want ErrNotSubscribed", err)
	}
}

func TestSubscriberIDMustBeUniquePerBroker(t *testing.T) {
	b := broker.New[string]()
	first := broker.NewSubscriber[string]("dup")
	second := broker.NewSubscriber[string]("dup")

	if err := first.Subscribe(b); err != nil {
		t.Fatal(err)
	}
	if err := second.Subscribe(b); !errors.Is(err, broker.ErrSubscriberIDTaken) {
		t.Fatalf("err = %v, want ErrSubscriberIDTaken", err)
	}
}

func TestSubscriberRejectsEmptyID(t *testing.T) {
	sub := broker.NewSubscriber[string]("")
	if err := sub.Subscribe(broker.New[string]()); !errors.Is(err, broker.ErrEmptySubscriberID) {
		t.Fatalf("err = %v, want ErrEmptySubscriberID", err)
	}
}

func TestSubscriberCannotJoinSecondBroker(t *testing.T) {
	sub := broker.NewSubscriber[string]("s1")
	if err := sub.Subscribe(broker.New[string]()); err != nil {
		t.Fatal(err)
	}
	if err := sub.Subscribe(broker.New[string]()); !errors.Is(err, broker.ErrAlreadySubscribed) {
		t.Fatalf("err = %v, want ErrAlreadySubscribed", err)
	}
}

func TestExampleSequence(t *testing.T) {
	b := broker.New[string]()
	pub := broker.NewPublisher(b)

	var all []string
	publish := func(from, to int) {
		for i := from; i <= to; i++ {
			msg := fmt.Sprintf("message-%d", i)
			pub.Publish(msg)
			all = append(all, msg)
		}
	}

	publish(1, 5)
	sub1 := broker.NewSubscriber[string]("subscriber-1")
	if err := sub1.Subscribe(b); err != nil {
		t.Fatal(err)
	}
	publish(6, 10)
	sub2 := broker.NewSubscriber[string]("subscriber-2")
	if err := sub2.Subscribe(b); err != nil {
		t.Fatal(err)
	}

	got1, got2 := readAll(t, sub1), readAll(t, sub2)
	if !slices.Equal(got1, all) || !slices.Equal(got2, all) {
		t.Fatalf("sub1 = %v\nsub2 = %v\nwant both = %v", got1, got2, all)
	}
	if total := len(got1) + len(got2); total != 20 {
		t.Fatalf("total messages read = %d, want 20", total)
	}
}
