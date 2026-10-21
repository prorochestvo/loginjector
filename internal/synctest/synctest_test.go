package synctest

import (
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSafeBuffer(t *testing.T) {
	t.Parallel()

	t.Run("write then string", func(t *testing.T) {
		t.Parallel()

		var sb SafeBuffer
		_, err := sb.Write([]byte("hello"))
		require.NoError(t, err)
		require.Equal(t, "hello", sb.String())
	})

	t.Run("write then read", func(t *testing.T) {
		t.Parallel()

		var sb SafeBuffer
		_, err := sb.Write([]byte("world"))
		require.NoError(t, err)

		out := make([]byte, 5)
		n, err := sb.Read(out)
		require.NoError(t, err)
		require.Equal(t, 5, n)
		require.Equal(t, "world", string(out))

		// next read must return io.EOF (buffer drained)
		_, err = sb.Read(out)
		require.ErrorIs(t, err, io.EOF)
	})

	t.Run("NewBufferString seeds content", func(t *testing.T) {
		t.Parallel()

		sb := NewBufferString("seeded")
		require.Equal(t, "seeded", sb.String())
	})

	t.Run("NewBuffer seeds content", func(t *testing.T) {
		t.Parallel()

		sb := NewBuffer([]byte("bytes"))
		require.Equal(t, "bytes", sb.String())
	})

	t.Run("zero value is usable", func(t *testing.T) {
		t.Parallel()

		var sb SafeBuffer
		_, err := sb.Write([]byte("zero"))
		require.NoError(t, err)
		require.Equal(t, "zero", sb.String())
	})

	t.Run("Bytes returns a copy", func(t *testing.T) {
		t.Parallel()

		var sb SafeBuffer
		_, err := sb.Write([]byte("original"))
		require.NoError(t, err)

		snapshot := sb.Bytes()
		require.Equal(t, []byte("original"), snapshot)

		// additional write must not change the earlier snapshot
		_, err = sb.Write([]byte("-appended"))
		require.NoError(t, err)

		require.Equal(t, []byte("original"), snapshot,
			"snapshot must not be corrupted by a subsequent Write")
		require.Equal(t, "original-appended", sb.String())
	})

	t.Run("concurrent writes are race-free", func(t *testing.T) {
		t.Parallel()

		const goroutines = 20
		const writesPerGoroutine = 50
		payload := []byte("x")

		var sb SafeBuffer
		var wg sync.WaitGroup
		for range goroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range writesPerGoroutine {
					_, err := sb.Write(payload)
					if err != nil {
						// signal failure without calling t.Fatal from a goroutine
						panic(err)
					}
				}
			}()
		}
		wg.Wait()

		total := goroutines * writesPerGoroutine * len(payload)
		require.Len(t, sb.Bytes(), total,
			"total bytes written must equal goroutines × writes × payload length")
	})

	t.Run("concurrent writes and reads are race-free", func(t *testing.T) {
		t.Parallel()

		const writers = 10
		const readers = 10
		const writesPerGoroutine = 40
		const readsPerGoroutine = 40
		payload := []byte("y")

		var sb SafeBuffer
		var wg sync.WaitGroup

		for range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range writesPerGoroutine {
					if _, err := sb.Write(payload); err != nil {
						panic(err)
					}
				}
			}()
		}

		for range readers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range readsPerGoroutine {
					// both Bytes() and String() exercise the read-side lock path.
					_ = sb.Bytes()
					_ = sb.String()
				}
			}()
		}

		wg.Wait()
		// Bytes() and String() do not consume data, so all written bytes must be present.
		total := writers * writesPerGoroutine * len(payload)
		require.Len(t, sb.Bytes(), total,
			"all written bytes must be present after concurrent reads")
	})
}
