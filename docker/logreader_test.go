package docker

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/beaker/runtime/logging"
)

func TestLogHeader(t *testing.T) {
	t.Run("EOF", func(t *testing.T) {
		r := NewLogReader(bytes.NewReader(nil))
		m, err := r.ReadMessage()
		assert.Nil(t, m)
		assert.Equal(t, io.EOF, err)
	})

	t.Run("Truncated", func(t *testing.T) {
		r := NewLogReader(bytes.NewReader([]byte{0x01, 0x00, 0x00, 0x00}))
		m, err := r.ReadMessage()
		assert.Nil(t, m)
		assert.Equal(t, io.ErrUnexpectedEOF, err)
	})

	t.Run("ReadError", func(t *testing.T) {
		r := NewLogReader(badReader{})
		m, err := r.ReadMessage()
		assert.Nil(t, m)
		assert.EqualError(t, err, "docker: error reading log header: oh no")
	})

	t.Run("InvalidStream", func(t *testing.T) {
		r := NewLogReader(bytes.NewReader([]byte{0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0}))
		m, err := r.ReadMessage()
		assert.Nil(t, m)
		assert.EqualError(t, err, "docker: unexpected log stream: 0x3")
	})
}

func TestLogMessage(t *testing.T) {
	// We round-trip the time through text to remove sub-nanosecond artifacts.
	logTimeStr := time.Now().Format(time.RFC3339Nano)
	logTime, _ := time.Parse(time.RFC3339Nano, logTimeStr)

	t.Run("EOF", func(t *testing.T) {
		r := NewLogReader(bytes.NewReader([]byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05}))
		m, err := r.ReadMessage()
		assert.Nil(t, m)
		assert.Equal(t, io.ErrUnexpectedEOF, err)
	})

	t.Run("ReadError", func(t *testing.T) {
		r := NewLogReader(io.MultiReader(
			bytes.NewReader([]byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05}),
			badReader{}))
		m, err := r.ReadMessage()
		assert.Nil(t, m)
		assert.EqualError(t, err, "docker: error reading log message: oh no")
	})

	t.Run("EmptyLine", func(t *testing.T) {
		r := NewLogReader(bytes.NewReader(message(1, "")))
		m, err := r.ReadMessage()
		assert.Nil(t, m)
		assert.EqualError(t, err, "docker: invalid log time: EOF")
	})

	t.Run("InvalidTimestamp", func(t *testing.T) {
		r := NewLogReader(bytes.NewReader(message(1, "abcd efgh")))
		m, err := r.ReadMessage()
		assert.Nil(t, m)
		assert.EqualError(t, err, `docker: invalid log time: parsing time "abcd" as "2006-01-02T15:04:05.999999999Z07:00": cannot parse "abcd" as "2006"`)
	})

	t.Run("Minimal", func(t *testing.T) {
		r := NewLogReader(bytes.NewReader(message(1, logTimeStr+" ")))
		m, err := r.ReadMessage()
		require.NotNil(t, m)
		require.NoError(t, err)
		assert.Equal(t, logging.Stdout, m.Stream)
		assert.Equal(t, logTime.UTC(), m.Time)
		assert.Equal(t, "", m.Text)

		// Final read should result in nil, nil
		m, err = r.ReadMessage()
		assert.Nil(t, m)
		assert.Equal(t, io.EOF, err)
	})

	t.Run("Multiple", func(t *testing.T) {
		m1 := message(1, logTimeStr+" First one thing...")
		m2 := message(2, logTimeStr+" ... and then another.")
		r := NewLogReader(bytes.NewReader(append(m1, m2...)))

		m, err := r.ReadMessage()
		require.NotNil(t, m)
		require.NoError(t, err)
		assert.Equal(t, logging.Stdout, m.Stream)
		assert.Equal(t, logTime.UTC(), m.Time)
		assert.Equal(t, "First one thing...", m.Text)

		m, err = r.ReadMessage()
		require.NotNil(t, m)
		require.NoError(t, err)
		assert.Equal(t, logging.Stderr, m.Stream)
		assert.Equal(t, logTime.UTC(), m.Time)
		assert.Equal(t, "... and then another.", m.Text)

		// Final read should result in nil, nil
		m, err = r.ReadMessage()
		assert.Nil(t, m)
		assert.Equal(t, io.EOF, err)
	})

	t.Run("Truncated", func(t *testing.T) {
		// Write more than 64k.
		buf := &bytes.Buffer{}
		for i := 0; i < 1025; i++ {
			buf.Write([]byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_"))
		}

		r := NewLogReader(bytes.NewReader(message(1, logTimeStr+" "+buf.String())))
		m, err := r.ReadMessage()
		require.NotNil(t, m)
		require.NoError(t, err)

		assert.Equal(t, logging.Stdout, m.Stream)
		assert.Equal(t, logTime.UTC(), m.Time)
		assert.LessOrEqual(t, len(m.Text), 64*1024)

		// Final read should result in nil, nil
		m, err = r.ReadMessage()
		assert.Nil(t, m)
		assert.Equal(t, io.EOF, err)
	})
}

func message(stream byte, s string) []byte {
	header := [8]byte{stream}
	binary.BigEndian.PutUint32(header[4:], uint32(len(s)))
	return append(header[:], []byte(s)...)
}

type badReader struct{}

// Read implements the io.Reader interface.
func (r badReader) Read(p []byte) (int, error) {
	return 0, errors.New("oh no")
}
