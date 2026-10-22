package loginjector

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCloseOrLogError(t *testing.T) {
	t.Parallel()

	t.Run("error written to writer", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		closeErr := errors.New("close failed")
		CloseOrLogError(&buf, &fixedCloser{err: closeErr})
		require.Contains(t, buf.String(), "close failed")
	})

	t.Run("nil close writes nothing", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		CloseOrLogError(&buf, &fixedCloser{err: nil})
		require.Empty(t, buf.String())
	})

	t.Run("nil writer does not panic and still closes", func(t *testing.T) {
		t.Parallel()

		closed := &trackingCloser{err: errors.New("oops")}
		require.NotPanics(t, func() {
			CloseOrLogError(nil, closed)
		})
		require.True(t, closed.called, "closer must be called even with nil writer")
	})

	t.Run("nil closer is no-op", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		require.NotPanics(t, func() {
			CloseOrLogError(&buf, nil)
		})
		require.Empty(t, buf.String())
	})
}

func TestCloseOrJoin(t *testing.T) {
	t.Parallel()

	t.Run("join into nil error", func(t *testing.T) {
		t.Parallel()

		closeErr := errors.New("disk gone")
		result := func() (err error) {
			defer CloseOrJoin(&err, &fixedCloser{err: closeErr})
			return nil
		}()
		require.ErrorIs(t, result, closeErr)
	})

	t.Run("join into existing error", func(t *testing.T) {
		t.Parallel()

		existingErr := errors.New("write failed")
		closeErr := errors.New("close failed")
		result := func() (err error) {
			defer CloseOrJoin(&err, &fixedCloser{err: closeErr})
			err = existingErr
			return err
		}()
		require.ErrorIs(t, result, existingErr)
		require.ErrorIs(t, result, closeErr)
	})

	t.Run("nil close leaves error untouched", func(t *testing.T) {
		t.Parallel()

		original := errors.New("original")
		result := func() (err error) {
			defer CloseOrJoin(&err, &fixedCloser{err: nil})
			err = original
			return err
		}()
		// errors.Join(original, nil) returns a joinError wrapping the original; use
		// ErrorIs to check the semantic value rather than the concrete type.
		require.ErrorIs(t, result, original)
	})

	t.Run("nil errp does not panic", func(t *testing.T) {
		t.Parallel()

		closed := &trackingCloser{err: errors.New("boom")}
		require.NotPanics(t, func() {
			CloseOrJoin(nil, closed)
		})
		require.True(t, closed.called, "closer must be called even with nil errp")
	})

	t.Run("nil closer is no-op", func(t *testing.T) {
		t.Parallel()

		var err error
		require.NotPanics(t, func() {
			CloseOrJoin(&err, nil)
		})
		require.NoError(t, err)
	})
}

// trackingCloser records whether Close was called.
var _ io.Closer = (*trackingCloser)(nil)

type trackingCloser struct {
	err    error
	called bool
}

func (tc *trackingCloser) Close() error {
	tc.called = true
	return tc.err
}

// fixedCloser is an io.Closer that returns a fixed error.
var _ io.Closer = (*fixedCloser)(nil)

type fixedCloser struct{ err error }

func (f *fixedCloser) Close() error { return f.err }
