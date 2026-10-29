// Package adapters provides an optional preset of named severity constants for the
// loginjector.LogLevel type. The core loginjector package ships no level constants by
// design — LogLevel is consumer-defined. This sub-package offers a ready-made,
// six-rung ladder (Debug..Critical = 1..6) whose values match the historical
// consumer-side copies, so existing projects can delete their hand-rolled declarations
// and replace them with a single import without renumbering call sites or persisted
// values. Importing this package is entirely opt-in; ignoring it leaves the
// unopinionated core exactly as it is.
package adapters

import (
	"strings"

	"github.com/prorochestvo/loginjector"
)

// Debug is the lowest severity level, used for verbose diagnostic output.
const Debug loginjector.LogLevel = 1

// Info is the standard informational level for routine operational messages.
const Info loginjector.LogLevel = 2

// Warning indicates a potentially harmful situation that does not yet require action.
const Warning loginjector.LogLevel = 3

// Error indicates a recoverable failure that should be investigated.
const Error loginjector.LogLevel = 4

// Severe indicates a serious failure that may affect system stability.
const Severe loginjector.LogLevel = 5

// Critical is the highest severity level, indicating an imminent or active system failure.
const Critical loginjector.LogLevel = 6

// Parse converts a string to the corresponding loginjector.LogLevel constant. The
// comparison is case-insensitive and leading/trailing whitespace is trimmed. The
// six canonical names ("debug", "info", "warning", "error", "severe", "critical") are
// accepted, as are the inbound-only aliases "warn" (maps to Warning) and "err" (maps
// to Error). Any unrecognized or empty input defaults to Info. Parse never returns a
// zero LogLevel — zero is intentionally below Debug and would suppress all log output.
func Parse(s string) loginjector.LogLevel {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return Debug
	case "info":
		return Info
	case "warning", "warn":
		return Warning
	case "error", "err":
		return Error
	case "severe":
		return Severe
	case "critical":
		return Critical
	default:
		return Info
	}
}

// Name returns the canonical lowercase name for the given loginjector.LogLevel. For
// the six defined constants (Debug..Critical) it returns the canonical name (e.g.
// "warning", never the alias "warn"). For any value outside the 1..6 range it returns
// the stable sentinel "unknown". Name and Parse form a round-trip for canonical names:
// Parse(Name(c)) == c for every defined constant.
func Name(l loginjector.LogLevel) string {
	switch l {
	case Debug:
		return "debug"
	case Info:
		return "info"
	case Warning:
		return "warning"
	case Error:
		return "error"
	case Severe:
		return "severe"
	case Critical:
		return "critical"
	default:
		return "unknown"
	}
}
