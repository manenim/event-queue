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

// Broker stores published messages and each subscriber's read position.
// It is safe for concurrent use. T is the payload type, so one broker can
// carry strings, structs or any other type. Create one with New.
type Broker[T any] struct {
	mu  sync.Mutex
	log []Message[T]
	// offsets maps a subscriber ID to the offset of the next message it
	// will read. Every message before that offset has been consumed.
	offsets map[string]int
}

// New returns an empty broker.
func New[T any]() *Broker[T] {
	return &Broker[T]{offsets: make(map[string]int)}
}

// Subscribe registers a subscriber ID. A new subscriber starts at offset 0,
// so it receives every message already in the log, then every later one.
//
// Subscribing an ID that is already registered is a no-op: its read
// position is kept, not reset. Subscribe reports whether the ID was newly
// registered.
func (b *Broker[T]) Subscribe(id string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.offsets[id]; exists {
		return false
	}
	b.offsets[id] = 0
	return true
}

// Read returns the next unread messages for subscriber id, in publish order,
// and moves the subscriber's offset past them. Each message is returned to a
// given subscriber at most once. max limits how many messages are returned;
// max <= 0 means no limit. An empty result means nothing new to read.
//
// Read returns ErrNotSubscribed if id has not subscribed.
func (b *Broker[T]) Read(id string, max int) ([]Message[T], error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	offset, ok := b.offsets[id]
	if !ok {
		return nil, ErrNotSubscribed
	}

	end := len(b.log)
	if max > 0 && offset+max < end {
		end = offset + max
	}
	if offset == end {
		return nil, nil
	}

	// Return a copy so callers can't change the broker's log through the slice.
	out := make([]Message[T], end-offset)
	copy(out, b.log[offset:end])
	b.offsets[id] = end
	return out, nil
}

// Pending returns how many messages subscriber id has not read yet.
func (b *Broker[T]) Pending(id string) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	offset, ok := b.offsets[id]
	if !ok {
		return 0, ErrNotSubscribed
	}
	return len(b.log) - offset, nil
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
