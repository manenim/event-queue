package broker_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/manenim/event-queue/broker"
)

func TestConcurrentPublishAndRead(t *testing.T) {
	const (
		publishers    = 8
		perPublisher  = 500
		subscribers   = 4
		totalMessages = publishers * perPublisher
	)

	b := broker.New[string]()
	subs := make([]*broker.Subscriber[string], subscribers)
	for i := range subs {
		subs[i] = broker.NewSubscriber[string](fmt.Sprintf("s%d", i))
		if err := subs[i].Subscribe(b); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pub := broker.NewPublisher(b)
			for i := 0; i < perPublisher; i++ {
				pub.Publish(fmt.Sprintf("p%d-%d", p, i))
			}
		}()
	}

	received := make([][]broker.Message[string], subscribers)
	var readers sync.WaitGroup
	for i, s := range subs {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for len(received[i]) < totalMessages {
				msgs, err := s.Read(7)
				if err != nil {
					t.Error(err)
					return
				}
				received[i] = append(received[i], msgs...)
			}
		}()
	}

	wg.Wait()
	readers.Wait()

	for i, msgs := range received {
		if len(msgs) != totalMessages {
			t.Fatalf("subscriber %d got %d messages, want %d", i, len(msgs), totalMessages)
		}
		for want, m := range msgs {
			if m.Offset != want {
				t.Fatalf("subscriber %d: message %d has offset %d (gap, repeat or reorder)", i, want, m.Offset)
			}
		}
	}
}

func TestConcurrentBatchesStayContiguous(t *testing.T) {
	const batches, batchSize = 50, 10

	b := broker.New[int]()
	var wg sync.WaitGroup
	for n := 0; n < batches; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := make([]int, batchSize)
			for i := range batch {
				batch[i] = n
			}
			b.PublishBatch(batch...)
		}()
	}
	wg.Wait()

	b.Subscribe("checker")
	msgs, err := b.Read("checker", 0)
	if err != nil {
		t.Fatal(err)
	}
	for start := 0; start < len(msgs); start += batchSize {
		for _, m := range msgs[start : start+batchSize] {
			if m.Payload != msgs[start].Payload {
				t.Fatalf("batch starting at offset %d was interleaved", start)
			}
		}
	}
}

func TestConcurrentSubscribeIsNoOp(t *testing.T) {
	b := broker.New[string]()
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		created int
	)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b.Subscribe("same") {
				mu.Lock()
				created++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if created != 1 {
		t.Fatalf("Subscribe registered the ID %d times, want 1", created)
	}
}
