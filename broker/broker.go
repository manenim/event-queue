// Package broker is an in-memory pub-sub broker: one append-only log,
// with a separate read offset per subscriber.
package broker

import (
	"sync"
	"time"
)

type Message[T any] struct {
	Offset      int
	Payload     T
	PublishedAt time.Time
}

type Broker[T any] struct {
	mu      sync.Mutex
	log     []Message[T]
	offsets map[string]int // subscriber ID -> offset of its next unread message
}

func New[T any]() *Broker[T] {
	return &Broker[T]{offsets: make(map[string]int)}
}

func (b *Broker[T]) Subscribe(id string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	// Already subscribed: no-op, keep the existing offset.
	if _, exists := b.offsets[id]; exists {
		return false
	}
	// Start at 0 so a late subscriber still sees the full history.
	b.offsets[id] = 0
	return true
}

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

	// Copy so callers can't modify the log, and advance the offset under the
	// same lock so no message is delivered twice.
	out := make([]Message[T], end-offset)
	copy(out, b.log[offset:end])
	b.offsets[id] = end
	return out, nil
}

func (b *Broker[T]) Pending(id string) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	offset, ok := b.offsets[id]
	if !ok {
		return 0, ErrNotSubscribed
	}
	return len(b.log) - offset, nil
}

func (b *Broker[T]) Publish(payload T) Message[T] {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.appendLocked(payload)
}

// PublishBatch appends all payloads under one lock, so the batch stays contiguous.
func (b *Broker[T]) PublishBatch(payloads ...T) []Message[T] {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]Message[T], 0, len(payloads))
	for _, p := range payloads {
		out = append(out, b.appendLocked(p))
	}
	return out
}

func (b *Broker[T]) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.log)
}

// appendLocked must be called with b.mu held.
func (b *Broker[T]) appendLocked(payload T) Message[T] {
	msg := Message[T]{
		Offset:      len(b.log),
		Payload:     payload,
		PublishedAt: time.Now(),
	}
	b.log = append(b.log, msg)
	return msg
}
