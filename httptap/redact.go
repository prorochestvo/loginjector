package httptap

import (
	"net/http"
	"strings"
)

// defaultRedactHeaders is the immutable baseline redaction set applied to every
// request and response logged by this package. It cannot be removed by callers;
// WithRedactHeaders is additive only.
var defaultRedactHeaders = []string{
	"Authorization",
	"Proxy-Authorization",
	"Cookie",
	"Set-Cookie",
	"X-Api-Key",
	"X-Auth-Token",
}

// redactKey normalizes a header name for redaction set matching: lowercase,
// then trimmed of whitespace. This ensures case-insensitive matching even
// when headers are written directly into the map with non-canonical keys.
func redactKey(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

// headerToStringWithRedact converts an http.Header map to a multi-line string,
// omitting any header whose normalized name (lowercase, trimmed) appears in the
// redact set. The logged key preserves its original spelling. Matching is
// case-insensitive: a non-canonical key written directly into w.Header()
// (e.g. "set-cookie") is still redacted.
func headerToStringWithRedact(header *http.Header, redact map[string]struct{}) string {
	var res strings.Builder
	for key, v := range *header {
		trimmedKey := strings.TrimSpace(key)
		if trimmedKey == "" {
			continue
		}
		if _, skip := redact[redactKey(trimmedKey)]; skip {
			continue
		}
		val := strings.Join(v, "; ")
		val = strings.TrimSpace(val)
		res.WriteString(trimmedKey)
		res.WriteString(": ")
		res.WriteString(val)
		res.WriteString("\n")
	}
	return res.String()
}
