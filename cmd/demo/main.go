package main

import (
	"fmt"
	"log"

	"github.com/manenim/event-queue/broker"
)

func main() {
	b := broker.New[string]()

	pub := broker.NewPublisher(b)

	for i := 1; i <= 5; i++ {
		pub.Publish(fmt.Sprintf("message-%d", i))
	}

	sub1 := broker.NewSubscriber[string]("subscriber-1")
	must(sub1.Subscribe(b))

	for i := 6; i <= 10; i++ {
		pub.Publish(fmt.Sprintf("message-%d", i))
	}

	sub2 := broker.NewSubscriber[string]("subscriber-2")
	must(sub2.Subscribe(b))

	total := 0
	for _, sub := range []*broker.Subscriber[string]{sub1, sub2} {
		msgs, err := sub.ReadAll()
		must(err)
		for _, m := range msgs {
			fmt.Printf("[%s] offset=%d payload=%s\n", sub.ID(), m.Offset, m.Payload)
		}
		total += len(msgs)
	}
	fmt.Printf("\nTotal message logs: %d\n", total)

	fmt.Println("\nChecks:")

	must(sub1.Subscribe(b))
	again, err := sub1.ReadAll()
	must(err)
	fmt.Printf("  subscriber-1 subscribes again, then reads: %d messages (re-subscribe is a no-op)\n", len(again))

	again, err = sub2.ReadAll()
	must(err)
	fmt.Printf("  subscriber-2 reads again: %d messages (nothing consumed twice)\n", len(again))
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
