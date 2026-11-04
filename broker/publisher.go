package broker

type Publisher[T any] struct {
	broker *Broker[T]
}

func NewPublisher[T any](b *Broker[T]) *Publisher[T] {
	return &Publisher[T]{broker: b}
}

func (p *Publisher[T]) Publish(payload T) Message[T] {
	return p.broker.Publish(payload)
}

func (p *Publisher[T]) PublishBatch(payloads ...T) []Message[T] {
	return p.broker.PublishBatch(payloads...)
}
