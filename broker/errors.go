package broker

import "errors"

var (
	// ErrNotSubscribed is returned when reading for a subscriber that has not subscribed.
	ErrNotSubscribed = errors.New("broker: subscriber is not subscribed")

	// ErrEmptySubscriberID is returned when subscribing with an empty ID.
	ErrEmptySubscriberID = errors.New("broker: subscriber ID must not be empty")

	// ErrSubscriberIDTaken is returned when a different subscriber already
	// uses the same ID on the broker. Sharing an ID would mean sharing a
	// read position, which breaks independent reads.
	ErrSubscriberIDTaken = errors.New("broker: subscriber ID already in use")

	// ErrAlreadySubscribed is returned when a subscriber that is attached to
	// one broker tries to subscribe to a different one.
	ErrAlreadySubscribed = errors.New("broker: subscriber is already subscribed to another broker")
)
