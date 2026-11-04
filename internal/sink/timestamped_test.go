package sink

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewTimestamped(t *testing.T) {
	t.Parallel()
	fixed := time.Date(2024, 3, 15, 10, 30, 45, 0, time.UTC)
	t.Run("single line", func(t *testing.T) {
		t.Parallel()
		var out bytes.Buffer
		f := NewTimestamped(&out, DefaultTimeLayout, func() time.Time { return fixed })

		n, err := f.Write([]byte("  message  \n"))

		require.NoError(t, err)
		require.Equal(t, len("  message  \n"), n)
		require.Equal(t, "2024/03/15 10:30:45 message\n", out.String())
	})
	t.Run("multi line indentation", func(t *testing.T) {
		t.Parallel()
		var out bytes.Buffer
		f := NewTimestamped(&out, DefaultTimeLayout, func() time.Time { return fixed })

		_, err := f.Write([]byte("first\nsecond\n"))

		require.NoError(t, err)
		require.Equal(t, "2024/03/15 10:30:45 first\n                    second\n", out.String())
	})
	t.Run("empty input", func(t *testing.T) {
		t.Parallel()
		var out bytes.Buffer
		f := NewTimestamped(&out, DefaultTimeLayout, func() time.Time { return fixed })

		_, err := f.Write(nil)

		require.NoError(t, err)
		require.Equal(t, "2024/03/15 10:30:45\n", out.String())
	})
	t.Run("fixed clock", func(t *testing.T) {
		t.Parallel()
		var out bytes.Buffer
		clock := func() time.Time { return fixed }
		f := NewTimestamped(&out, "15:04:05", clock)

		_, err := f.Write([]byte("message"))

		require.NoError(t, err)
		require.Equal(t, "10:30:45 message\n", out.String())
	})
}
