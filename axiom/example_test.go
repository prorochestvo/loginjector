package axiom_test

import (
	"os"

	"github.com/prorochestvo/loginjector"
	"github.com/prorochestvo/loginjector/axiom"
	"github.com/prorochestvo/loginjector/levels"
)

func ExampleNewHandler() {
	ax, err := axiom.NewHandler(os.Getenv("AXIOM_TOKEN"), "app-logs",
		axiom.WithDomain("eu-central-1.aws.edge.axiom.co"),
		axiom.WithFields(map[string]any{"service": "beacon", "env": "prod"}))
	if err != nil {
		return
	}
	defer ax.Close()
	logger := loginjector.NewLogger(levels.Info, loginjector.TimestampedPrintHandler(), ax)
	std := logger.StdLog(levels.Info, "")
	std.Printf("started")
}

func ExampleHandler_withMinLevel() {
	ax, err := axiom.NewHandler(os.Getenv("AXIOM_TOKEN"), "app-logs")
	if err != nil {
		return
	}
	defer ax.Close()

	warnings := loginjector.WithMinLevel(levels.Warning, ax)
	logger := loginjector.NewLogger(levels.Info, warnings)
	logger.Printf(levels.Info, "not queued for Axiom")
	logger.Printf(levels.Warning, "queued for Axiom")
}
