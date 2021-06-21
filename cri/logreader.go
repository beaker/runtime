package cri

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/sirupsen/logrus"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"

	"github.com/beaker/runtime/logging"
)

var (
	// EOL markers separate log lines. These appear regardless of whether a a
	// log line is partial or full, and should be removed for partial lines.
	eol = []byte{'\n'}

	// Delimiters separate timestamp, stream type, and log message fields on each line.
	delimiter = []byte{' '}

	// Tag delimiters separate log tags if more than one appear in a line.
	tagDelimiter = []byte(cri.LogTagDelimiter)
)

// LogReader translates streamed CRI logs into discrete, structured log
// messages. This reader is not safe for concurrent use.
type LogReader struct {
	r     io.Reader
	since time.Time

	buf   *bufio.Reader
	parse parseFunc
}

// NewLogReader wraps a streaming log reader. The provided reader must
// include timestamps.
//
// The reader introduces its own buffering and may read data from r beyond the
// bytes requested by Read().
func NewLogReader(r io.Reader, since time.Time) *LogReader {
	lr := &LogReader{r: r, buf: bufio.NewReader(r), since: since}
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
	// Implementation adapted from cri-tools:
	// https://github.com/kubernetes-sigs/cri-tools

	msg := &logging.Message{}
	for {
		l, err := r.buf.ReadBytes(eol[0])
		if err != nil {
			if err == io.EOF {
				if len(l) == 0 {
					// File ended normally.
					return nil, io.EOF
				}
				// File ended in a partial line.
				return nil, io.ErrUnexpectedEOF
			}
			return nil, fmt.Errorf("cri: failed to read log: %w", err)
		}

		if r.parse == nil {
			// Initialize the log parsing function.
			if r.parse, err = getParseFunc(l); err != nil {
				return nil, fmt.Errorf("cri: %w", err)
			}
		}

		if err := r.parse(l, msg); err != nil {
			// Log and ignore bad lines.
			logrus.WithError(err).Error("Failed to parse log line")
			continue
		}

		// Skip lines before the start time.
		if !msg.Time.Before(r.since) {
			break
		}
	}

	msg.Time = msg.Time.UTC() // TODO: Should we leave this to the caller?
	return msg, nil
}

// parseFunc is a function parsing one log line to the internal log type.
// Notice that the caller must make sure logMessage is not nil.
type parseFunc func(log []byte, msg *logging.Message) error

var parseFuncs = []parseFunc{
	parseCRILog,        // CRI log format
	parseDockerJSONLog, // Docker JSON log format
}

// getParseFunc returns proper parse function based on the sample log line passed in.
func getParseFunc(log []byte) (parseFunc, error) {
	for _, p := range parseFuncs {
		if err := p(log, &logging.Message{}); err == nil {
			return p, nil
		}
	}
	return nil, fmt.Errorf("unsupported log format: %q", log)
}

// parseCRILog parses logs in CRI log format.
//
// Example:
//   2016-10-06T00:17:09.669794202Z stdout P log content 1
//   2016-10-06T00:17:09.669794203Z stderr F log content 2
func parseCRILog(log []byte, msg *logging.Message) error {
	var err error

	// Parse the timestamp.
	idx := bytes.Index(log, delimiter)
	if idx < 0 {
		return fmt.Errorf("timestamp is not found")
	}
	msg.Time, err = time.Parse(time.RFC3339Nano, string(log[:idx]))
	if err != nil {
		return fmt.Errorf("unexpected timestamp format %q: %v", time.RFC3339Nano, err)
	}

	// Parse stream type.
	log = log[idx+1:]
	idx = bytes.Index(log, delimiter)
	if idx < 0 {
		return fmt.Errorf("stream type is not found")
	}
	switch s := cri.LogStreamType(log[:idx]); s {
	case cri.Stdout:
		msg.Stream = logging.Stdout
	case cri.Stderr:
		msg.Stream = logging.Stderr
	default:
		return fmt.Errorf("unexpected stream type %q", s)
	}

	// Parse log tag.
	log = log[idx+1:]
	idx = bytes.Index(log, delimiter)
	if idx < 0 {
		return fmt.Errorf("log tag is not found")
	}
	// Keep this forward compatible.
	tags := bytes.Split(log[:idx], tagDelimiter)
	partial := cri.LogTag(tags[0]) == cri.LogTagPartial
	// Trim the tailing new line if this is a partial line.
	if partial && len(log) > 0 && log[len(log)-1] == '\n' {
		log = log[:len(log)-1]
	}

	msg.Text = string(log[idx+1:])
	return nil
}

// parseDockerJSONLog parses logs in Docker JSON log format.
//
// Example:
//   {"log":"content 1","stream":"stdout","time":"2016-10-20T18:39:20.57606443Z"}
//   {"log":"content 2","stream":"stderr","time":"2016-10-20T18:39:20.57606444Z"}
func parseDockerJSONLog(log []byte, msg *logging.Message) error {
	var jsonMsg struct {
		// Log is the log message
		Log string `json:"log,omitempty"`
		// Stream is the log source
		Stream string `json:"stream,omitempty"`
		// Created is the created timestamp of log
		Created time.Time `json:"time"`
	}

	if err := json.Unmarshal(log, &jsonMsg); err != nil {
		return fmt.Errorf("failed with %w to unmarshal log %q", err, log)
	}

	switch s := cri.LogStreamType(jsonMsg.Stream); s {
	case "", cri.Stdout:
		msg.Stream = logging.Stdout
	case cri.Stderr:
		msg.Stream = logging.Stderr
	default:
		return fmt.Errorf("unexpected stream type %q", s)
	}
	msg.Time = jsonMsg.Created
	msg.Text = jsonMsg.Log
	return nil
}
