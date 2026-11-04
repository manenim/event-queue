package broker

import "errors"

var (
	// ErrNotSubscribed is returned when reading for a subscriber that has not subscribed.
	ErrNotSubscribed = errors.New("broker: subscriber is not subscribed")
)
