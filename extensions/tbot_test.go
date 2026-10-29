package extensions

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTelegramHandler(t *testing.T) {
	t.Parallel()
	t.Run("delivers to the live API", func(t *testing.T) {
		t.Parallel()
		botToken := os.Getenv("TELEGRAM_BOT_TOKEN")
		chatID := os.Getenv("TELEGRAM_BOT_CHAT_ID")

		if botToken == "" || chatID == "" {
			t.Skipf("TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_CHAT_ID not set")
		}

		m := time.Now().UTC().Format(time.RFC3339) + ": 14C225CB-9BE0-40D8-8FB3-6218FE17AE53"

		h := TelegramHandler(botToken, chatID, "test.log", "LogInjector", "<b>demo</b> of telegram handler")
		_, err := h.Write([]byte(m))
		require.NoError(t, err)
	})

	t.Run("redacts the bot token from a surfaced request error", func(t *testing.T) {
		t.Parallel()

		// "%zz" is an invalid URL escape, so http.NewRequest fails deterministically and
		// fast without any network — and net/url echoes the offending URL (token included)
		// verbatim into the error string, exercising the redactToken closure on the
		// request-build return site. The token is >= 8 chars so the guard does not skip it.
		botToken := "1234567890ABCDEF%zz"

		h := TelegramHandler(botToken, "chat", "test.log", "label")
		_, err := h.Write([]byte("payload"))

		require.Error(t, err)
		require.NotContains(t, err.Error(), botToken,
			"the bot token must never appear in the surfaced error")
		require.Contains(t, err.Error(), "***",
			"the redaction placeholder must replace the token")
		require.Contains(t, err.Error(), "bot***/sendDocument",
			"the literal bot prefix stays; only the secret is masked")
	})

	t.Run("short token is left unredacted by the guard", func(t *testing.T) {
		t.Parallel()

		// a token below the 8-char guard is not redacted (it is not a valid Telegram token
		// and would over-match unrelated substrings). "%zz" still forces the build error.
		botToken := "%zz"

		h := TelegramHandler(botToken, "chat", "test.log", "label")
		_, err := h.Write([]byte("payload"))

		require.Error(t, err)
		require.NotContains(t, err.Error(), "***",
			"a sub-8-char token must not be redacted")
	})
}
