package runtime

import (
	"errors"
)

var (
	// ErrNotFound indicates an operation was attempted on a nonexistent container.
	ErrNotFound = errors.New("container not found")

	// ErrNotStarted indicates failure because a container hasn't started yet.
	ErrNotStarted = errors.New("container has not started")

	// ErrNotImplemented indicates the underlying runtime hasn't implemented a function.
	ErrNotImplemented = errors.New("not implemented")
)
