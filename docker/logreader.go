package docker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	"github.com/beaker/runtime/logging"
)

// LogReader translates streamed Docker logs into discrete, structured log
// messages. This reader is not safe for concurrent use.
type LogReader struct {
	r     io.Reader
	inBuf bytes.Buffer
}

// NewLogReader wraps a streaming Docker log reader. The provided reader must
// include timestamps.
//
// The reader introduces its own buffering and may read data from r beyond the
// bytes requested by Read().
func NewLogReader(r io.Reader) *LogReader {
	lr := &LogReader{r: r}
	return lr
}

// Close implements the io.Closer interface.
func (r *LogReader) Close() error {
	if c, ok := r.r.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// ReadMessage implements the logging.LogReader interface.
func (r *LogReader) ReadMessage() (*logging.Message, error) {
	const maxMsgLen = 64 * 1024

	stream, size, err := r.readHeader()
	if err != nil {
		return nil, err
	}

	// Discard the part of the line which is over the limit.
	var discard int64
	if size > maxMsgLen {
		discard = size - maxMsgLen
		size = maxMsgLen
	}

	// Read the message text, then discard extra bytes.
	r.inBuf.Reset()
	if _, err := io.CopyN(&r.inBuf, r.r, size); err != nil {
		return nil, readErr(err)
	}
	if _, err := io.CopyN(ioutil.Discard, r.r, discard); err != nil {
		return nil, readErr(err)
	}

	// Parse out the timestamp. Though Docker also ensures time stamps have
	// constant length, we instead rely on the fact that Docker formats logs as
	// "[header][time] [text]".
	ts, err := r.inBuf.ReadString(' ')
	if err != nil {
		return nil, fmt.Errorf("docker: invalid log time: %w", err)
	}
	t, err := time.Parse(time.RFC3339Nano, ts[:len(ts)-1])
	if err != nil {
		return nil, fmt.Errorf("docker: invalid log time: %w", err)
	}

	return &logging.Message{Stream: stream, Time: t.UTC(), Text: r.inBuf.String()}, nil
}

// Header layout is as follows. For more detail, see the Docker repository:
// https://github.com/moby/moby/blob/v17.03.2-ce/pkg/stdcopy/stdcopy.go
//
// +---------------------------------------+
// | 0  | 1  | 2  | 3  | 4  | 5  | 6  | 7  |
// | FD | 0            | text length       |
// +-------------------------------------- +
func (r *LogReader) readHeader() (stream logging.IOStream, size int64, err error) {
	var header [8]byte // Fixed-size buffer so we don't need to allocate.

	if _, err := io.ReadFull(r.r, header[:]); err != nil {
		switch err {
		case io.EOF, io.ErrUnexpectedEOF:
			return 0, 0, err
		default:
			return 0, 0, fmt.Errorf("docker: error reading log header: %w", err)
		}
	}

	stream = logging.IOStream(header[0])
	if stream != logging.Stdout && stream != logging.Stderr {
		return 0, 0, fmt.Errorf("docker: unexpected log stream: %#x", stream)
	}

	size = int64(binary.BigEndian.Uint32(header[4:8]))
	return stream, size, nil
}

func readErr(err error) error {
	switch err {
	case nil:
		return nil
	case io.EOF, io.ErrUnexpectedEOF:
		// Logs should not terminate mid-message, so all EOFs are unexpected.
		return io.ErrUnexpectedEOF
	default:
		return fmt.Errorf("docker: error reading log message: %w", err)
	}
}
