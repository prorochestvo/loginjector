package axiom_test

import (
	"os"

	"github.com/prorochestvo/loginjector"
	"github.com/prorochestvo/loginjector/adapters"
	"github.com/prorochestvo/loginjector/axiom"
)

func ExampleNewHandler() {
	ax, err := axiom.NewHandler(
		axiom.Token(
			os.Getenv("AXIOM_TOKEN"),
		),
		"app-logs",
		axiom.WithDomain("eu-central-1.aws.edge.axiom.co"),
		axiom.WithFields(map[string]any{"service": "beacon", "env": "prod"}),
	)
	if err != nil {
		return
	}
	defer ax.Close()
	logger := loginjector.NewLogger(
		adapters.Info,
		loginjector.TimestampedPrintHandler(),
		ax,
	)
	std := logger.StdLog(adapters.Info, "")
	std.Printf("started")
}

func ExampleHandler_withMinLevel() {
	ax, err := axiom.NewHandler(axiom.Token(os.Getenv("AXIOM_TOKEN")), "app-logs")
	if err != nil {
		return
	}
	defer ax.Close()

	warnings := loginjector.WithMinLevel(adapters.Warning, ax)
	logger := loginjector.NewLogger(adapters.Info, warnings)
	logger.Printf(adapters.Info, "not queued for Axiom")
	logger.Printf(adapters.Warning, "queued for Axiom")
}
