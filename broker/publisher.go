package broker

// Publisher writes messages to a broker.
type Publisher[T any] struct {
	broker *Broker[T]
}

// NewPublisher returns a publisher that writes to b.
func NewPublisher[T any](b *Broker[T]) *Publisher[T] {
	return &Publisher[T]{broker: b}
}

// Publish writes one message and returns it as stored, with its offset.
func (p *Publisher[T]) Publish(payload T) Message[T] {
	return p.broker.Publish(payload)
}

// PublishBatch writes several messages as one contiguous, ordered run.
func (p *Publisher[T]) PublishBatch(payloads ...T) []Message[T] {
	return p.broker.PublishBatch(payloads...)
}
