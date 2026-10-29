package loginjector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"github.com/prorochestvo/loginjector/internal/sink"
)

func TelegramHandler(botToken, chatID, fileName string, labels ...string) io.Writer {
	const telegramAPI = "https://api.telegram.org/bot"
	url := fmt.Sprintf("%s%s/sendDocument", telegramAPI, botToken)
	// redactToken masks botToken in an error string before it is surfaced: a failed
	// request build or client.Do returns a *url.Error whose text embeds the full URL,
	// token included, which would otherwise land in logs and log files.
	redactToken := func(s string) string {
		// guard: an empty or very short token would over-match unrelated substrings, and
		// no real Telegram token is that short, so skip redaction below the threshold.
		if len(botToken) < 8 {
			return s
		}
		return strings.ReplaceAll(s, botToken, "***")
	}
	w := sink.NewFunc(func(msg []byte) (int, error) {
		payload := &bytes.Buffer{}
		parts := multipart.NewWriter(payload)

		if filePart, err := parts.CreateFormFile("document", fileName); err != nil {
			return 0, fmt.Errorf("could not create request form file, details: %w", err)
		} else if _, err = filePart.Write(msg); err != nil {
			return 0, fmt.Errorf("could not write file part to request, details: %w", err)
		}

		if err := parts.WriteField("chat_id", chatID); err != nil {
			return 0, fmt.Errorf("could not write chat_id field to request: %w", err)
		}

		caption := strings.Join(labels, "\n")
		caption = fmt.Sprintf("%s %s", time.Now().UTC().Format("2006-01-02 15:04:05"), caption)
		caption = strings.TrimSpace(caption)
		if err := parts.WriteField("caption", caption); err != nil {
			return 0, fmt.Errorf("could not write caption field to request: %w", err)
		}

		if err := parts.WriteField("parse_mode", "HTML"); err != nil {
			return 0, fmt.Errorf("could not write parse_mode field to request: %w", err)
		}

		contentType := parts.FormDataContentType()

		if err := parts.Close(); err != nil {
			return 0, fmt.Errorf("could not close request: %w", err)
		}

		request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, payload)
		if err != nil {
			return 0, fmt.Errorf("could not create HTTP request: %v", redactToken(err.Error()))
		}
		request.Header.Set("Content-Type", contentType)

		r, err := (&http.Client{Timeout: time.Second * 20}).Do(request)
		if err != nil {
			return 0, fmt.Errorf("could not send HTTP request: %v", redactToken(err.Error()))
		}
		// close error is discarded; logging here would re-enter the logger.
		defer func() { _ = r.Body.Close() }()

		if r.StatusCode != http.StatusOK {
			return 0, fmt.Errorf("could not to deliver message, status code: %v", r.StatusCode)
		}

		var response struct {
			Ok bool `json:"ok"`
		}
		rawResponse := bytes.NewBufferString("")
		err = json.NewDecoder(io.TeeReader(r.Body, rawResponse)).Decode(&response)
		if err != nil || !response.Ok {
			//nolint:errorlint // err may be nil when response.Ok is false; %w would render %!w(<nil>)
			return 0, fmt.Errorf("could not decode response: %v\n%s", err, rawResponse.String())
		}

		return len(msg), nil
	})
	return w
}

// rotatingFileConfig holds the resolved configuration for RotatingFileHandler.
