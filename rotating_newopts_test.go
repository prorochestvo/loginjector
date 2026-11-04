package loginjector

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestWithMaxAgeDays covers the days-unit wrapper: it must resolve to the identical
// cfg.maxAge as the equivalent WithMaxAge span, zero must disable pruning, and it must
// actually age out an old backup on rotation.
func TestWithMaxAgeDays(t *testing.T) {
	t.Parallel()
	t.Run("resolves to the same maxAge as the equivalent WithMaxAge", func(t *testing.T) {
		t.Parallel()

		var got, want rotatingFileConfig
		WithMaxAgeDays(14)(&got)
		WithMaxAge(14 * 24 * time.Hour)(&want)
		require.Equal(t, want.maxAge, got.maxAge,
			"WithMaxAgeDays(14) must equal WithMaxAge(14 days)")
	})

	t.Run("zero disables age pruning", func(t *testing.T) {
		t.Parallel()

		var cfg rotatingFileConfig
		WithMaxAgeDays(0)(&cfg)
		require.LessOrEqual(t, cfg.maxAge, time.Duration(0),
			"WithMaxAgeDays(0) must delegate to the zero-disables path")
	})

	t.Run("day bound prunes a backup older than it, keeps a fresh one", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		touchOld(t, filepath.Join(dir, idxName("app", 1)), "o1\n", 48*time.Hour) // older than 1 day
		touchOld(t, filepath.Join(dir, idxName("app", 2)), "f2\n", 0)            // fresh

		var cfg rotatingFileConfig
		cfg.maxFiles = 100
		WithMaxAgeDays(1)(&cfg)
		require.NoError(t, pruneRotation(dir, "app", cfg, idxName("app", 3), 0, time.Now()))

		require.NoFileExists(t, filepath.Join(dir, idxName("app", 1)),
			"the 48h-old backup must be pruned under a 1-day age bound")
		require.FileExists(t, filepath.Join(dir, idxName("app", 2)),
			"a fresh backup must survive the 1-day age bound")
	})
}
