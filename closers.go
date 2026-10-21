package loginjector

import (
	"errors"
	"fmt"
	"io"
	"log"
)

// CloseOrPanic closes the closer and panics if there is an error.
func CloseOrPanic(closer io.Closer) {
	err := closer.Close()
	if err != nil {
		panic(err)
	}
}

// CloseOrLog closes the closer and logs the error if there is one.
func CloseOrLog(closer io.Closer) {
	err := closer.Close()
	if err != nil {
		log.Println(err)
	}
}

// CloseOrLogError closes c and, if Close returns a non-nil error and w is non-nil,
// writes the error message followed by a newline to w. It differs from CloseOrLog in
// that it writes to the caller-supplied writer rather than the standard library log
// package, making it suitable for structured logging (e.g. logger.WriterAs(level)).
// A nil w means the write is skipped but c is still closed. A nil c is a no-op.
// The write error is intentionally discarded — writing to a logger is the documented
// fmt.Fprint*-to-logger exception to the no-skipped-errors rule.
func CloseOrLogError(w io.Writer, c io.Closer) {
	if c == nil {
		return
	}
	err := c.Close()
	if err != nil && w != nil {
		// write error intentionally discarded: fmt.Fprint*-to-logger exception.
		_, _ = fmt.Fprintln(w, err)
	}
}

// CloseOrJoin closes c and joins any close error into *errp using errors.Join, which
// drops nil arguments and returns nil when all arguments are nil. The intended usage is
//
//	defer CloseOrJoin(&err, c)
//
// inside a function with a named (... err error) return — the deferred call runs after
// the function body and joins the close error into whatever err holds at that point.
// A nil errp means the close error has nowhere to be stored; c is still closed and the
// error is silently discarded. A nil c is a no-op.
//
//nolint:gocritic // errp is written through by design; part of the public CloseOrJoin API
func CloseOrJoin(errp *error, c io.Closer) {
	if c == nil {
		return
	}
	closeErr := c.Close()
	if errp == nil {
		return
	}
	*errp = errors.Join(*errp, closeErr)
}
