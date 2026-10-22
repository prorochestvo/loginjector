package httptap

import (
	"net/http"
	"net/url"
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

// defaultRedactQueryParams is the immutable baseline set of query parameter names
// whose values are redacted when logged. The set covers standard OAuth/API credential
// parameter names. It cannot be removed by callers; WithRedactQueryParams is additive only.
var defaultRedactQueryParams = map[string]struct{}{
	"access_token":  {},
	"refresh_token": {},
	"id_token":      {},
	"client_secret": {},
	"api_key":       {},
	"apikey":        {},
	"password":      {},
}

// redactKey normalizes a header or query parameter name for redaction set
// matching: trimmed of whitespace, then lowercased. Header keys written
// directly into the map with non-canonical spelling still match.
func redactKey(name string) string {
	name = strings.TrimSpace(name)
	return strings.ToLower(name)
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

// redactRawQuery masks credential values in a URL query string, keeping the
// parameter shape debuggable. It splits on both & and ; delimiters, matches
// parameter names case-insensitively after percent-decoding, and returns the
// query unchanged if no parameter matches. Non-matching segments are emitted
// byte-for-byte. A parameter without a value (no '=') is kept unchanged.
func redactRawQuery(raw string, names map[string]struct{}) string {
	if len(names) == 0 || raw == "" {
		return raw
	}

	// the first pass looks for a match so a query without one is returned without allocating.
	hasMatch := false
	i := 0
	for i < len(raw) {
		nextDelim := len(raw)
		if k := strings.IndexAny(raw[i:], "&;"); k >= 0 {
			nextDelim = i + k
		}

		segment := raw[i:nextDelim]
		eqIdx := strings.Index(segment, "=")
		if eqIdx >= 0 {
			name := segment[:eqIdx]
			decodedName, err := url.QueryUnescape(name)
			if err != nil {
				decodedName = name
			}
			if _, redact := names[redactKey(decodedName)]; redact {
				hasMatch = true
				break
			}
		}

		if nextDelim < len(raw) {
			i = nextDelim + 1
		} else {
			i = nextDelim
		}
	}

	if !hasMatch {
		return raw
	}

	var result strings.Builder
	i = 0
	for i < len(raw) {
		nextDelim := len(raw)
		if k := strings.IndexAny(raw[i:], "&;"); k >= 0 {
			nextDelim = i + k
		}

		segment := raw[i:nextDelim]
		eqIdx := strings.Index(segment, "=")
		if eqIdx < 0 {
			result.WriteString(segment)
		} else {
			name := segment[:eqIdx]
			decodedName, err := url.QueryUnescape(name)
			if err != nil {
				decodedName = name
			}
			if _, redact := names[redactKey(decodedName)]; redact {
				result.WriteString(name)
				result.WriteString("=[REDACTED]")
			} else {
				result.WriteString(segment)
			}
		}

		if nextDelim < len(raw) {
			result.WriteByte(raw[nextDelim])
			i = nextDelim + 1
		} else {
			i = nextDelim
		}
	}

	return result.String()
}
