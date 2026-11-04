// Command demo runs the example sequence from the assessment brief:
//
//  1. Create a Broker.
//  2. Create a Publisher.
//  3. Publish five distinct messages.
//  4. Create Subscriber 1 and subscribe it to the Broker.
//  5. Publish five additional distinct messages.
//  6. Create Subscriber 2 and subscribe it to the Broker.
//  7. Read and print the messages for both subscribers.
//
// Expected outcome: each subscriber prints all 10 messages in publication
// order, giving 20 message lines in total.
package main

import (
	"fmt"
	"log"

	"github.com/manenim/event-queue/broker"
)

func main() {
	// 1. Create a Broker.
	b := broker.New[string]()

	// 2. Create a Publisher.
	pub := broker.NewPublisher(b)

	// 3. Publish five distinct messages.
	for i := 1; i <= 5; i++ {
		pub.Publish(fmt.Sprintf("message-%d", i))
	}

	// 4. Create Subscriber 1 and subscribe it to the Broker.
	sub1 := broker.NewSubscriber[string]("subscriber-1")
	must(sub1.Subscribe(b))

	// 5. Publish five additional distinct messages.
	for i := 6; i <= 10; i++ {
		pub.Publish(fmt.Sprintf("message-%d", i))
	}

	// 6. Create Subscriber 2 and subscribe it to the Broker.
	sub2 := broker.NewSubscriber[string]("subscriber-2")
	must(sub2.Subscribe(b))

	// 7. Read and print the messages for both subscribers.
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

	// Extra checks for the remaining requirements.
	fmt.Println("\nChecks:")

	// Subscribing again is a no-op: no error, and the read position is kept.
	must(sub1.Subscribe(b))
	again, err := sub1.ReadAll()
	must(err)
	fmt.Printf("  subscriber-1 subscribes again, then reads: %d messages (re-subscribe is a no-op)\n", len(again))

	// Each message is consumed at most once per subscriber.
	again, err = sub2.ReadAll()
	must(err)
	fmt.Printf("  subscriber-2 reads again: %d messages (nothing consumed twice)\n", len(again))
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
