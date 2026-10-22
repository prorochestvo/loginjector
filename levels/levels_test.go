package levels_test

import (
	"testing"

	"github.com/prorochestvo/loginjector"
	"github.com/prorochestvo/loginjector/levels"
	"github.com/stretchr/testify/require"
)

//nolint:testifylint // expected-actual misreads the typed literal as actual; the constants are under test
func TestConstantValues(t *testing.T) {
	t.Parallel()

	require.Equal(t, loginjector.LogLevel(1), levels.Debug)
	require.Equal(t, loginjector.LogLevel(2), levels.Info)
	require.Equal(t, loginjector.LogLevel(3), levels.Warning)
	require.Equal(t, loginjector.LogLevel(4), levels.Error)
	require.Equal(t, loginjector.LogLevel(5), levels.Severe)
	require.Equal(t, loginjector.LogLevel(6), levels.Critical)
}

func TestParse(t *testing.T) {
	t.Parallel()

	t.Run("canonical names", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			input string
			want  loginjector.LogLevel
		}{
			{"debug", levels.Debug},
			{"info", levels.Info},
			{"warning", levels.Warning},
			{"error", levels.Error},
			{"severe", levels.Severe},
			{"critical", levels.Critical},
		}
		for _, tc := range cases {
			t.Run(tc.input, func(t *testing.T) {
				t.Parallel()
				require.Equal(t, tc.want, levels.Parse(tc.input))
			})
		}
	})

	t.Run("case insensitive", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, levels.Debug, levels.Parse("DEBUG"))
		require.Equal(t, levels.Info, levels.Parse("INFO"))
		require.Equal(t, levels.Warning, levels.Parse("WARNING"))
		require.Equal(t, levels.Error, levels.Parse("ERROR"))
		require.Equal(t, levels.Severe, levels.Parse("SEVERE"))
		require.Equal(t, levels.Critical, levels.Parse("CRITICAL"))
		require.Equal(t, levels.Info, levels.Parse(" INFO "))
	})

	t.Run("whitespace trimmed", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, levels.Debug, levels.Parse("  debug  "))
		require.Equal(t, levels.Critical, levels.Parse("\tcritical\n"))
	})

	t.Run("unknown defaults to info", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, levels.Info, levels.Parse("bogus"))
		require.Equal(t, levels.Info, levels.Parse("trace"))
		require.Equal(t, levels.Info, levels.Parse("FATAL"))
	})

	t.Run("empty defaults to info", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, levels.Info, levels.Parse(""))
		require.Equal(t, levels.Info, levels.Parse("   "))
	})

	t.Run("aliases warn and err", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, levels.Warning, levels.Parse("warn"))
		require.Equal(t, levels.Warning, levels.Parse("WARN"))
		require.Equal(t, levels.Error, levels.Parse("err"))
		require.Equal(t, levels.Error, levels.Parse("ERR"))
	})
}

func TestName(t *testing.T) {
	t.Parallel()

	t.Run("each constant renders canonical", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			level loginjector.LogLevel
			want  string
		}{
			{levels.Debug, "debug"},
			{levels.Info, "info"},
			{levels.Warning, "warning"},
			{levels.Error, "error"},
			{levels.Severe, "severe"},
			{levels.Critical, "critical"},
		}
		for _, tc := range cases {
			t.Run(tc.want, func(t *testing.T) {
				t.Parallel()
				require.Equal(t, tc.want, levels.Name(tc.level))
			})
		}
	})

	t.Run("out of range returns sentinel", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, "unknown", levels.Name(loginjector.LogLevel(0)))
		require.Equal(t, "unknown", levels.Name(loginjector.LogLevel(99)))
		require.Equal(t, "unknown", levels.Name(loginjector.LogLevel(-1)))
	})

	t.Run("name never emits aliases", func(t *testing.T) {
		t.Parallel()

		require.NotEqual(t, "warn", levels.Name(levels.Warning))
		require.NotEqual(t, "err", levels.Name(levels.Error))
	})
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()

	constants := []loginjector.LogLevel{
		levels.Debug,
		levels.Info,
		levels.Warning,
		levels.Error,
		levels.Severe,
		levels.Critical,
	}
	for _, c := range constants {
		t.Run(levels.Name(c), func(t *testing.T) {
			t.Parallel()
			require.Equal(t, c, levels.Parse(levels.Name(c)))
		})
	}
}
