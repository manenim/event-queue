// Package broker implements an in-memory publish-subscribe message broker.
//
// Every published message is stored once, in an append-only log. Each
// subscriber has its own read position (offset) into that log, so reading
// with one subscriber never affects another.
package broker

import (
	"sync"
	"time"
)

// Message is a single entry in the broker's log.
type Message[T any] struct {
	// Offset is the message's position in the log. It starts at 0 and grows
	// by one per published message, so it also records publish order.
	Offset      int
	Payload     T
	PublishedAt time.Time
}

// Broker stores published messages. It is safe for concurrent use.
// T is the payload type, so one broker can carry strings, structs or any
// other type. Create one with New.
type Broker[T any] struct {
	mu  sync.Mutex
	log []Message[T]
}

// New returns an empty broker.
func New[T any]() *Broker[T] {
	return &Broker[T]{}
}

// Publish appends payload to the end of the log and returns the stored message.
func (b *Broker[T]) Publish(payload T) Message[T] {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.appendLocked(payload)
}

// PublishBatch appends all payloads in one step. The batch lands in the log
// as one contiguous run, with no messages from other publishers in between.
func (b *Broker[T]) PublishBatch(payloads ...T) []Message[T] {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]Message[T], 0, len(payloads))
	for _, p := range payloads {
		out = append(out, b.appendLocked(p))
	}
	return out
}

// Len returns the number of messages in the log.
func (b *Broker[T]) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.log)
}

// appendLocked adds one message to the log. The caller must hold b.mu.
func (b *Broker[T]) appendLocked(payload T) Message[T] {
	msg := Message[T]{
		Offset:      len(b.log),
		Payload:     payload,
		PublishedAt: time.Now(),
	}
	b.log = append(b.log, msg)
	return msg
}
