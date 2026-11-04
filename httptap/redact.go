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
}

// headerToStringWithRedact converts an http.Header map to a multi-line string,
// omitting any header whose canonical name appears in the redact set. Keys in
// redact must already be canonicalised with http.CanonicalHeaderKey; keys
// sourced from an http.Header are always canonical, so no second canonicalisation
// is needed here.
func headerToStringWithRedact(header *http.Header, redact map[string]struct{}) string {
	var res strings.Builder
	for key, v := range *header {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, skip := redact[key]; skip {
			continue
		}
		val := strings.Join(v, "; ")
		val = strings.TrimSpace(val)
		res.WriteString(key)
		res.WriteString(": ")
		res.WriteString(val)
		res.WriteString("\n")
	}
	return res.String()
}
