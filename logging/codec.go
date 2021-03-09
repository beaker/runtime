package logging

import (
	"errors"
	"io"

	"github.com/vmihailenco/msgpack"
)

const fieldCount = 3

// An Encoder writes structured log messages to an output stream.
type Encoder struct {
	e *msgpack.Encoder
}

// NewEncoder returns a new encoder that writes to w.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{e: msgpack.NewEncoder(w).UseCompactEncoding(true)}
}

// Encode writes the binary encoding of v to the stream.
func (e *Encoder) Encode(v Message) error {
	if v.Stream != Stdout && v.Stream != Stderr {
		return errors.New("logging: invalid IO stream")
	}

	if err := e.e.EncodeArrayLen(fieldCount); err != nil {
		return err
	}
	if err := e.e.EncodeInt(int64(v.Stream)); err != nil {
		return err
	}
	if err := e.e.EncodeTime(v.Time); err != nil {
		return err
	}
	if err := e.e.EncodeString(v.Text); err != nil {
		return err
	}
	return nil
}

// A Decoder reads and decodes structured log messages from an input stream.
type Decoder struct {
	d *msgpack.Decoder
}

// NewDecoder returns a new decoder that reads from r.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{d: msgpack.NewDecoder(r)}
}

// Decode reads the next value from its input and stores it in the value pointed to by v.
func (d *Decoder) Decode(v *Message) error {
	l, err := d.d.DecodeArrayLen()
	if err != nil {
		return err
	}
	if l != fieldCount {
		return errors.New("logging: possible corruption or invalid encoding")
	}

	stream, err := d.d.DecodeInt()
	if err != nil {
		return err
	}
	v.Stream = IOStream(stream)
	if v.Stream != Stdout && v.Stream != Stderr {
		return errors.New("logging: invalid IO stream")
	}

	t, err := d.d.DecodeTime()
	if err != nil {
		return err
	}
	v.Time = t.UTC()

	v.Text, err = d.d.DecodeString()
	return err
}
