package logging

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCodec(t *testing.T) {
	message := Message{
		Stream: Stdout,
		Time:   time.Date(1983, time.June, 21, 8, 42, 0, 12345, time.UTC),
		Text:   "Coming back to where you started is not the same as never leaving.", // Pratchett
	}

	var buf bytes.Buffer
	require.NoError(t, NewEncoder(&buf).Encode(message))
	assert.Equal(t, []byte{
		0x93,                                                       // Array length
		0x01,                                                       // Stream
		0xd7, 0xff, 0x00, 0x00, 0xc0, 0xe4, 0x19, 0x55, 0x7c, 0xd8, // Timestamp
		0xd9, 0x42, // Text header
		'C', 'o', 'm', 'i', 'n', 'g', ' ', 'b', 'a', 'c', 'k', ' ',
		't', 'o', ' ', 'w', 'h', 'e', 'r', 'e', ' ', 'y', 'o', 'u',
		' ', 's', 't', 'a', 'r', 't', 'e', 'd', ' ', 'i', 's', ' ',
		'n', 'o', 't', ' ', 't', 'h', 'e', ' ', 's', 'a', 'm', 'e',
		' ', 'a', 's', ' ', 'n', 'e', 'v', 'e', 'r', ' ', 'l', 'e',
		'a', 'v', 'i', 'n', 'g', '.',
	}, buf.Bytes())

	var out Message
	require.NoError(t, NewDecoder(&buf).Decode(&out))
	assert.Equal(t, message, out)
}
