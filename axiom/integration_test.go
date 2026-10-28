//go:build axiom_integration

package axiom_test

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prorochestvo/loginjector"
	"github.com/prorochestvo/loginjector/axiom"
	"github.com/prorochestvo/loginjector/levels"
	"github.com/stretchr/testify/require"
)

func TestIntegration_smokeIngest(t *testing.T) {
	token := os.Getenv("AXIOM_TOKEN")
	dataset := os.Getenv("AXIOM_DATASET")
	domain := os.Getenv("AXIOM_DOMAIN")
	if token == "" || dataset == "" || domain == "" {
		t.Skip("set AXIOM_TOKEN, AXIOM_DATASET, and AXIOM_DOMAIN to run the Axiom integration test")
	}

	handler, err := axiom.NewHandler(
		axiom.Token(token),
		axiom.Dataset(dataset),
		axiom.WithDomain(domain),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := handler.Close(); err != nil {
			t.Errorf("close handler: %v", err)
		}
	})

	runID := time.Now().UTC().Format(time.RFC3339Nano)
	t.Logf("Axiom integration run ID: %s", runID)

	for _, event := range []struct {
		level   loginjector.LogLevel
		message string
	}{
		{level: levels.Info, message: "info event"},
		{level: levels.Warning, message: "warning event"},
		{level: levels.Error, message: "error event"},
	} {
		_, err := handler.WriteLevel(event.level, []byte(fmt.Sprintf("%s %s", runID, event.message)))
		require.NoError(t, err)
	}

	const payloadBytes = 500 << 10
	prefix := runID + " "
	padding := strings.Repeat(
		"x",
		payloadBytes-len(prefix),
	)
	for range 8 {
		_, err := handler.Write([]byte(prefix + padding))
		require.NoError(t, err)
	}

	require.NoError(
		t,
		handler.Close(),
	)
	stats := handler.Stats()
	require.EqualValues(t, 0, stats.Failed)
	require.EqualValues(t, 0, stats.Dropped)
	require.EqualValues(t, 11, stats.Sent)
}
