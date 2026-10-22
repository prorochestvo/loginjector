package httptap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEscapeControl(t *testing.T) {
	t.Parallel()

	t.Run("plain_ascii_unchanged", func(t *testing.T) {
		t.Parallel()
		input := "hello-world"
		assert.Equal(t, input, escapeControl(input))
	})

	t.Run("non_ascii_unchanged", func(t *testing.T) {
		t.Parallel()
		input := "/café"
		assert.Equal(t, input, escapeControl(input))
	})

	t.Run("newline_encoded", func(t *testing.T) {
		t.Parallel()
		input := "hello\nworld"
		expected := "hello%0Aworld"
		assert.Equal(t, expected, escapeControl(input))
	})

	t.Run("cr_tab_nul_del", func(t *testing.T) {
		t.Parallel()
		// Test CR, TAB, NUL, DEL
		input := "a\rb\tc\x00d\x7Fe"
		output := escapeControl(input)
		assert.Contains(t, output, "%0D") // CR
		assert.Contains(t, output, "%09") // TAB
		assert.Contains(t, output, "%00") // NUL
		assert.Contains(t, output, "%7F") // DEL
		assert.NotContains(t, output, "\r")
		assert.NotContains(t, output, "\t")
		assert.NotContains(t, output, "\x00")
		assert.NotContains(t, output, "\x7F")
	})

	t.Run("unicode_line_separators", func(t *testing.T) {
		t.Parallel()
		// U+2028 (line separator) and U+2029 (paragraph separator)
		// These are multibyte UTF-8 sequences
		input := "a b c"
		output := escapeControl(input)
		// U+2028 encodes as E2 80 A8 in UTF-8
		assert.Contains(t, output, "%E2%80%A8")
		// U+2029 encodes as E2 80 A9 in UTF-8
		assert.Contains(t, output, "%E2%80%A9")
	})

	t.Run("invalid_utf8_unchanged", func(t *testing.T) {
		t.Parallel()
		// A lone 0xFF byte is not valid UTF-8, but it's not a control byte either
		// It should pass through unchanged
		input := "a\xFFb"
		output := escapeControl(input)
		assert.Equal(t, input, output)
	})
}
