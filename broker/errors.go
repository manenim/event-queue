package broker

import "errors"

var (
	ErrNotSubscribed     = errors.New("broker: subscriber is not subscribed")
	ErrEmptySubscriberID = errors.New("broker: subscriber ID must not be empty")
	ErrSubscriberIDTaken = errors.New("broker: subscriber ID already in use")
	ErrAlreadySubscribed = errors.New("broker: subscriber is already subscribed to another broker")
)
