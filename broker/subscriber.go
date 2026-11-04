package broker

import "sync"

// Subscriber reads messages from a broker. Its read position is kept by the
// broker under the subscriber's ID, so it survives across reads and is never
// shared with other subscribers.
type Subscriber[T any] struct {
	id string

	mu     sync.Mutex
	broker *Broker[T] // nil until Subscribe succeeds
}

// NewSubscriber returns a subscriber with the given ID. The ID must be unique
// within a broker.
func NewSubscriber[T any](id string) *Subscriber[T] {
	return &Subscriber[T]{id: id}
}

// ID returns the subscriber's ID.
func (s *Subscriber[T]) ID() string { return s.id }

// Subscribe attaches the subscriber to b. Calling it again with the same
// broker is a no-op that returns nil and keeps the read position.
//
// It returns ErrEmptySubscriberID if the ID is empty, ErrAlreadySubscribed if
// the subscriber is already attached to a different broker, and
// ErrSubscriberIDTaken if another subscriber already uses this ID on b.
func (s *Subscriber[T]) Subscribe(b *Broker[T]) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch {
	case s.broker == b:
		return nil // already subscribed here: no-op
	case s.broker != nil:
		return ErrAlreadySubscribed
	case s.id == "":
		return ErrEmptySubscriberID
	}
	if !b.Subscribe(s.id) {
		return ErrSubscriberIDTaken
	}
	s.broker = b
	return nil
}

// Read returns up to max unread messages in publish order (max <= 0 means
// all of them). It returns ErrNotSubscribed if Subscribe has not been called.
func (s *Subscriber[T]) Read(max int) ([]Message[T], error) {
	s.mu.Lock()
	b := s.broker
	s.mu.Unlock()

	if b == nil {
		return nil, ErrNotSubscribed
	}
	return b.Read(s.id, max)
}

// ReadAll returns every unread message in publish order.
func (s *Subscriber[T]) ReadAll() ([]Message[T], error) {
	return s.Read(0)
}

// Pending returns how many messages this subscriber has not read yet.
func (s *Subscriber[T]) Pending() (int, error) {
	s.mu.Lock()
	b := s.broker
	s.mu.Unlock()

	if b == nil {
		return 0, ErrNotSubscribed
	}
	return b.Pending(s.id)
}
