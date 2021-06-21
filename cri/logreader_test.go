package cri

import (
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/beaker/runtime/logging"
)

func TestLogReader(t *testing.T) {
	t.Run("EmptyLog", func(t *testing.T) {
		r := NewLogReader(strings.NewReader(""), time.Time{})
		m, err := r.ReadMessage()
		assert.Equal(t, io.EOF, err)
		assert.Nil(t, m)
	})

	t.Run("UnexpectedEOF", func(t *testing.T) {
		r := NewLogReader(strings.NewReader("no line ending!"), time.Time{})
		m, err := r.ReadMessage()
		assert.Equal(t, io.ErrUnexpectedEOF, err)
		assert.Nil(t, m)
	})

	t.Run("EmptyLog", func(t *testing.T) {
		r := NewLogReader(strings.NewReader(""), time.Time{})
		m, err := r.ReadMessage()
		assert.Equal(t, io.EOF, err)
		assert.Nil(t, m)
	})

	t.Run("InvalidFormat", func(t *testing.T) {
		r := NewLogReader(strings.NewReader("foobar\n"), time.Time{})
		m, err := r.ReadMessage()
		assert.EqualError(t, err, `cri: unsupported log format: "foobar\n"`)
		assert.Nil(t, m)
	})

	t.Run("ReadError", func(t *testing.T) {
		r := NewLogReader(badReader{}, time.Time{})
		m, err := r.ReadMessage()
		assert.EqualError(t, err, "cri: failed to read log: oh no")
		assert.Nil(t, m)
	})

}

func TestCRILogFormat(t *testing.T) {
	// We round-trip the time through text to remove sub-nanosecond artifacts.
	logTimeStr := time.Now().Format(time.RFC3339Nano)
	logTime, _ := time.Parse(time.RFC3339Nano, logTimeStr)

	t.Run("EmptyLine", func(t *testing.T) {
		r := NewLogReader(strings.NewReader(logTimeStr+" stdout P \n"), time.Time{})
		m, err := r.ReadMessage()
		require.NoError(t, err)
		assert.Equal(t, &logging.Message{Stream: logging.Stdout, Time: logTime.UTC(), Text: ""}, m)
	})

	t.Run("MultipleLines", func(t *testing.T) {
		r := NewLogReader(strings.NewReader(
			logTimeStr+" stdout P First one thing...\n"+
				logTimeStr+" stdout F  and then another\n"+
				logTimeStr+" stderr F This is an error\n",
		), time.Time{})

		m, err := r.ReadMessage()
		require.NoError(t, err)
		assert.Equal(t, &logging.Message{
			Stream: logging.Stdout,
			Time:   logTime.UTC(),
			Text:   "First one thing...",
		}, m)

		m, err = r.ReadMessage()
		require.NoError(t, err)
		assert.Equal(t, &logging.Message{
			Stream: logging.Stdout,
			Time:   logTime.UTC(),
			Text:   " and then another\n",
		}, m)

		m, err = r.ReadMessage()
		require.NoError(t, err)
		assert.Equal(t, &logging.Message{
			Stream: logging.Stderr,
			Time:   logTime.UTC(),
			Text:   "This is an error\n",
		}, m)

		m, err = r.ReadMessage()
		assert.Nil(t, m)
		assert.Equal(t, io.EOF, err)
	})

	t.Run("Since", func(t *testing.T) {
		r := NewLogReader(strings.NewReader(
			logTime.Add(-1).Format(time.RFC3339Nano)+" stdout F This should be skipped.\n"+
				logTime.Format(time.RFC3339Nano)+" stdout F This is the first message.\n",
		), logTime)

		m, err := r.ReadMessage()
		require.NoError(t, err)
		assert.Equal(t, &logging.Message{
			Stream: logging.Stdout,
			Time:   logTime.UTC(),
			Text:   "This is the first message.\n",
		}, m)

		m, err = r.ReadMessage()
		assert.Nil(t, m)
		assert.Equal(t, io.EOF, err)
	})
}

func TestJSONLogFormat(t *testing.T) {
	// We round-trip the time through text to remove sub-nanosecond artifacts.
	logTimeStr := time.Now().Format(time.RFC3339Nano)
	logTime, _ := time.Parse(time.RFC3339Nano, logTimeStr)

	t.Run("EmptyLine", func(t *testing.T) {
		r := NewLogReader(strings.NewReader(
			`{"time":"`+logTimeStr+`"}`+"\n",
		), time.Time{})
		m, err := r.ReadMessage()
		require.NoError(t, err)
		assert.Equal(t, &logging.Message{Stream: logging.Stdout, Time: logTime.UTC(), Text: ""}, m)
	})

	t.Run("MultipleLines", func(t *testing.T) {
		r := NewLogReader(strings.NewReader(
			`{"time":"`+logTimeStr+`","stream":"stdout","log":"First one thing..."}`+"\n"+
				`{"time":"`+logTimeStr+`","stream":"stdout","log":" and then another\n"}`+"\n"+
				`{"time":"`+logTimeStr+`","stream":"stderr","log":"This is an error\n"}`+"\n",
		), time.Time{})

		m, err := r.ReadMessage()
		require.NoError(t, err)
		assert.Equal(t, &logging.Message{
			Stream: logging.Stdout,
			Time:   logTime.UTC(),
			Text:   "First one thing...",
		}, m)

		m, err = r.ReadMessage()
		require.NoError(t, err)
		assert.Equal(t, &logging.Message{
			Stream: logging.Stdout,
			Time:   logTime.UTC(),
			Text:   " and then another\n",
		}, m)

		m, err = r.ReadMessage()
		require.NoError(t, err)
		assert.Equal(t, &logging.Message{
			Stream: logging.Stderr,
			Time:   logTime.UTC(),
			Text:   "This is an error\n",
		}, m)

		m, err = r.ReadMessage()
		assert.Nil(t, m)
		assert.Equal(t, io.EOF, err)
	})

	t.Run("Since", func(t *testing.T) {
		r := NewLogReader(strings.NewReader(
			`{"time":"`+logTime.Add(-1).Format(time.RFC3339Nano)+`","stream":"stdout","log":"This should be skipped.\n"}`+"\n"+
				`{"time":"`+logTimeStr+`","stream":"stdout","log":"This is the first message.\n"}`+"\n",
		), logTime)

		m, err := r.ReadMessage()
		require.NoError(t, err)
		assert.Equal(t, &logging.Message{
			Stream: logging.Stdout,
			Time:   logTime.UTC(),
			Text:   "This is the first message.\n",
		}, m)

		m, err = r.ReadMessage()
		assert.Nil(t, m)
		assert.Equal(t, io.EOF, err)
	})
}

type badReader struct{}

// Read implements the io.Reader interface.
func (r badReader) Read(p []byte) (int, error) {
	return 0, errors.New("oh no")
}
