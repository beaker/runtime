package logging

import (
	"io"
	"time"
)

// IOStream represents a Unix standard stream as a flag.
type IOStream int

// IOStream flag definitions
const (
	Stdin IOStream = iota
	Stdout
	Stderr
)

// A Message is a structured log message. Text includes trailing newlines if present.
type Message struct {
	Stream IOStream
	Time   time.Time
	Text   string
}

// LogReader provides ReadMessage() which reads a structured log message in
// sequential order as emitted by the container.
type LogReader interface {
	io.Closer

	// ReadMessage reads the next log message emitted by container.
	// Note: Time field in message is set in UTC.
	//
	// Returns nil, io.EOF if all log messages emitted by container
	// have been consumed.
	ReadMessage() (*Message, error)
}
