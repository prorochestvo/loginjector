package extensions

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHeaderToStringWithRedact(t *testing.T) {
	t.Parallel()

	t.Run("non_canonical_set_cookie_redacted", func(t *testing.T) {
		t.Parallel()
		h := http.Header{
			"set-cookie": {"s=x"},
		}
		output := headerToStringWithRedact(&h, map[string]struct{}{
			"set-cookie": {},
		})
		assert.NotContains(t, output, "s=x")
	})

	t.Run("mixed_case_authorization_redacted", func(t *testing.T) {
		t.Parallel()
		h := http.Header{
			"aUtHoRiZaTiOn": {"token123"},
		}
		output := headerToStringWithRedact(&h, map[string]struct{}{
			"authorization": {},
		})
		assert.NotContains(t, output, "token123")
	})

	t.Run("padded_key_redacted", func(t *testing.T) {
		t.Parallel()
		h := http.Header{
			" Cookie ": {"session=abc"},
		}
		output := headerToStringWithRedact(&h, map[string]struct{}{
			"cookie": {},
		})
		assert.NotContains(t, output, "session=abc")
	})

	t.Run("non_redacted_key_kept_verbatim", func(t *testing.T) {
		t.Parallel()
		h := http.Header{
			"X-Trace-Id": {"abc123"},
		}
		output := headerToStringWithRedact(&h, map[string]struct{}{})
		assert.Contains(t, output, "X-Trace-Id")
		assert.Contains(t, output, "abc123")
	})
}

func TestRedactRawQuery(t *testing.T) {
	t.Parallel()

	t.Run("no_match_byte_identical", func(t *testing.T) {
		t.Parallel()
		raw := "a=1&b=%20x"
		redacted := redactRawQuery(raw, map[string]struct{}{})
		assert.Equal(t, raw, redacted)
	})

	t.Run("default_access_token_masked", func(t *testing.T) {
		t.Parallel()
		raw := "access_token=secret123&page=2"
		redacted := redactRawQuery(raw, map[string]struct{}{
			"access_token": {},
		})
		assert.Contains(t, redacted, "access_token=[REDACTED]")
		assert.Contains(t, redacted, "page=2")
		assert.NotContains(t, redacted, "secret123")
	})

	t.Run("case_insensitive_name", func(t *testing.T) {
		t.Parallel()
		raw := "Access_Token=secret123"
		redacted := redactRawQuery(raw, map[string]struct{}{
			"access_token": {},
		})
		assert.Contains(t, redacted, "Access_Token=[REDACTED]")
		assert.NotContains(t, redacted, "secret123")
	})

	t.Run("percent_encoded_name", func(t *testing.T) {
		t.Parallel()
		raw := "access%5Ftoken=secret123"
		redacted := redactRawQuery(raw, map[string]struct{}{
			"access_token": {},
		})
		assert.Contains(t, redacted, "access%5Ftoken=[REDACTED]")
		assert.NotContains(t, redacted, "secret123")
	})

	t.Run("semicolon_separator", func(t *testing.T) {
		t.Parallel()
		raw := "a=1;password=p"
		redacted := redactRawQuery(raw, map[string]struct{}{
			"password": {},
		})
		assert.Contains(t, redacted, "a=1;password=[REDACTED]")
		assert.NotContains(t, redacted, "password=p")
	})

	t.Run("repeated_param_all_masked", func(t *testing.T) {
		t.Parallel()
		raw := "token=first&token=second"
		redacted := redactRawQuery(raw, map[string]struct{}{
			"token": {},
		})
		assert.Contains(t, redacted, "token=[REDACTED]&token=[REDACTED]")
	})

	t.Run("name_without_value_kept", func(t *testing.T) {
		t.Parallel()
		raw := "a=1&flag"
		redacted := redactRawQuery(raw, map[string]struct{}{
			"flag": {},
		})
		assert.Equal(t, raw, redacted)
	})

	t.Run("empty_segments_kept", func(t *testing.T) {
		t.Parallel()
		raw := "a=1&&b=2"
		redacted := redactRawQuery(raw, map[string]struct{}{
			"token": {},
		})
		assert.Equal(t, raw, redacted)
	})

	t.Run("invalid_escape_in_name_falls_back_to_raw", func(t *testing.T) {
		t.Parallel()
		raw := "a%ZZbad=secret"
		redacted := redactRawQuery(raw, map[string]struct{}{
			"a%zzbad": {},
		})
		// With invalid escape sequence, QueryUnescape returns an error and falls back
		// to the raw name; lowercasing "a%ZZbad" gives "a%zzbad" which matches
		assert.Contains(t, redacted, "a%ZZbad=[REDACTED]")
		assert.NotContains(t, redacted, "secret")
	})
}
