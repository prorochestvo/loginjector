package httptap

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	loginjector "github.com/prorochestvo/loginjector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestNewPayloadHandlerWithOptions_HeaderRedaction(t *testing.T) {
	t.Parallel()

	t.Run("response_header_set_directly_in_map_is_redacted", func(t *testing.T) {
		t.Parallel()

		b := bytes.NewBufferString("")
		l := loginjector.NewLogger(logLevelInfo, b)

		token := uniqueToken()
		req := httptest.NewRequest(http.MethodGet, "/test", bytes.NewBufferString(""))
		res := httptest.NewRecorder()

		nextFunc := func(w http.ResponseWriter, r *http.Request) {
			defer func() { _ = r.Body.Close() }()
			w.Header()["set-cookie"] = []string{"session=" + token}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("body"))
		}

		h := NewPayloadHandler(l, logLevelInfo, nextFunc)
		h(res, req)

		require.Equal(t, http.StatusOK, res.Code)
		assert.NotContains(t, b.String(), token, "set-cookie token should be redacted")
	})
}
