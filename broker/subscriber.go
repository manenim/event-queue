package broker

import "sync"

type Subscriber[T any] struct {
	id string

	mu     sync.Mutex
	broker *Broker[T]
}

func NewSubscriber[T any](id string) *Subscriber[T] {
	return &Subscriber[T]{id: id}
}

func (s *Subscriber[T]) ID() string { return s.id }

func (s *Subscriber[T]) Subscribe(b *Broker[T]) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch {
	case s.broker == b:
		return nil
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

func (s *Subscriber[T]) Read(max int) ([]Message[T], error) {
	s.mu.Lock()
	b := s.broker
	s.mu.Unlock()

	if b == nil {
		return nil, ErrNotSubscribed
	}
	return b.Read(s.id, max)
}

func (s *Subscriber[T]) ReadAll() ([]Message[T], error) {
	return s.Read(0)
}

func (s *Subscriber[T]) Pending() (int, error) {
	s.mu.Lock()
	b := s.broker
	s.mu.Unlock()

	if b == nil {
		return 0, ErrNotSubscribed
	}
	return b.Pending(s.id)
}
