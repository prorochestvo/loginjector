package extensions_test

import (
	"net/http"
	"time"

	"github.com/prorochestvo/loginjector/extensions"
)

// ExampleNewAccessHandler shows the canonical production access-log sink: a
// RotatingFileHandler configured for a stable, tail -F-able path with age-based retention
// and gzipped backups, handed to NewAccessHandler. RotatingFileHandler returns a
// mutex-guarded writer, so it satisfies NewAccessHandler's requirement that out be safe
// for concurrent Write calls.
func ExampleNewAccessHandler() {
	// access.log stays at a fixed path so `tail -F access.log` follows it across rotations;
	// rotated backups are indexed, gzipped, and pruned once older than two weeks.
	out := extensions.RotatingFileHandler(
		"./logs", "access",
		extensions.WithStableCurrentName(),
		extensions.WithMaxAge(14*24*time.Hour),
		extensions.WithCompress(),
	)

	app := func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}

	// Register the returned handler with http.Handle or your router.
	_ = extensions.NewAccessHandler(out, app)
}
