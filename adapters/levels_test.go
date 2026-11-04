package adapters_test

import (
	"testing"

	"github.com/prorochestvo/loginjector"
	"github.com/prorochestvo/loginjector/adapters"
	"github.com/stretchr/testify/require"
)

//nolint:testifylint // expected-actual misreads the typed literal as actual; the constants are under test
func TestConstantValues(t *testing.T) {
	t.Parallel()

	require.Equal(t, loginjector.LogLevel(1), adapters.Debug)
	require.Equal(t, loginjector.LogLevel(2), adapters.Info)
	require.Equal(t, loginjector.LogLevel(3), adapters.Warning)
	require.Equal(t, loginjector.LogLevel(4), adapters.Error)
	require.Equal(t, loginjector.LogLevel(5), adapters.Severe)
	require.Equal(t, loginjector.LogLevel(6), adapters.Critical)
}

func TestParse(t *testing.T) {
	t.Parallel()

	t.Run("canonical names", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			input string
			want  loginjector.LogLevel
		}{
			{"debug", adapters.Debug},
			{"info", adapters.Info},
			{"warning", adapters.Warning},
			{"error", adapters.Error},
			{"severe", adapters.Severe},
			{"critical", adapters.Critical},
		}
		for _, tc := range cases {
			t.Run(tc.input, func(t *testing.T) {
				t.Parallel()
				require.Equal(t, tc.want, adapters.Parse(tc.input))
			})
		}
	})

	t.Run("case insensitive", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, adapters.Debug, adapters.Parse("DEBUG"))
		require.Equal(t, adapters.Info, adapters.Parse("INFO"))
		require.Equal(t, adapters.Warning, adapters.Parse("WARNING"))
		require.Equal(t, adapters.Error, adapters.Parse("ERROR"))
		require.Equal(t, adapters.Severe, adapters.Parse("SEVERE"))
		require.Equal(t, adapters.Critical, adapters.Parse("CRITICAL"))
		require.Equal(t, adapters.Info, adapters.Parse(" INFO "))
	})

	t.Run("whitespace trimmed", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, adapters.Debug, adapters.Parse("  debug  "))
		require.Equal(t, adapters.Critical, adapters.Parse("\tcritical\n"))
	})

	t.Run("unknown defaults to info", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, adapters.Info, adapters.Parse("bogus"))
		require.Equal(t, adapters.Info, adapters.Parse("trace"))
		require.Equal(t, adapters.Info, adapters.Parse("FATAL"))
	})

	t.Run("empty defaults to info", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, adapters.Info, adapters.Parse(""))
		require.Equal(t, adapters.Info, adapters.Parse("   "))
	})

	t.Run("aliases warn and err", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, adapters.Warning, adapters.Parse("warn"))
		require.Equal(t, adapters.Warning, adapters.Parse("WARN"))
		require.Equal(t, adapters.Error, adapters.Parse("err"))
		require.Equal(t, adapters.Error, adapters.Parse("ERR"))
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
			{adapters.Debug, "debug"},
			{adapters.Info, "info"},
			{adapters.Warning, "warning"},
			{adapters.Error, "error"},
			{adapters.Severe, "severe"},
			{adapters.Critical, "critical"},
		}
		for _, tc := range cases {
			t.Run(tc.want, func(t *testing.T) {
				t.Parallel()
				require.Equal(t, tc.want, adapters.Name(tc.level))
			})
		}
	})

	t.Run("out of range returns sentinel", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, "unknown", adapters.Name(loginjector.LogLevel(0)))
		require.Equal(t, "unknown", adapters.Name(loginjector.LogLevel(99)))
		require.Equal(t, "unknown", adapters.Name(loginjector.LogLevel(-1)))
	})

	t.Run("name never emits aliases", func(t *testing.T) {
		t.Parallel()

		require.NotEqual(t, "warn", adapters.Name(adapters.Warning))
		require.NotEqual(t, "err", adapters.Name(adapters.Error))
	})
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()

	constants := []loginjector.LogLevel{
		adapters.Debug,
		adapters.Info,
		adapters.Warning,
		adapters.Error,
		adapters.Severe,
		adapters.Critical,
	}
	for _, c := range constants {
		t.Run(adapters.Name(c), func(t *testing.T) {
			t.Parallel()
			require.Equal(t, c, adapters.Parse(adapters.Name(c)))
		})
	}
}
