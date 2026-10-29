package sink

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	_ io.Writer = (*Func)(nil)
)

func TestNewFunc(t *testing.T) {
	t.Parallel()
	t.Run("forwards bytes", func(t *testing.T) {
		t.Parallel()
		var got []byte
		f := NewFunc(func(p []byte) (int, error) {
			got = append(got, p...)
			return len(p), nil
		})

		n, err := f.Write([]byte("message"))

		require.NoError(t, err)
		require.Equal(t, len("message"), n)
		require.Equal(t, []byte("message"), got)
	})
	t.Run("forwards errors", func(t *testing.T) {
		t.Parallel()
		want := errors.New("write failed")
		f := NewFunc(func([]byte) (int, error) { return 3, want })

		n, err := f.Write([]byte("message"))

		require.Equal(t, 3, n)
		require.ErrorIs(t, err, want)
	})
}

func TestFunc_WriteForRaceCondition(t *testing.T) {
	t.Parallel()
	const goroutines = 24
	const writes = 100
	const message = "intact line\n"

	var buf bytes.Buffer
	f := NewFunc(buf.Write)
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range writes {
				_, _ = f.Write([]byte(message))
			}
		}()
	}
	wg.Wait()

	require.Equal(t, goroutines*writes*len(message), buf.Len())
	require.Equal(t, goroutines*writes, bytes.Count(buf.Bytes(), []byte(message)))
}
