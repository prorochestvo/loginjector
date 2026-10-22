package httptap

import (
	"bytes"
	"unicode/utf8"
)

// escapeControl percent-encodes control characters (0x00–0x1F, 0x7F) and
// Unicode line/paragraph separators (U+0085, U+2028, U+2029) in the input string.
// Returns the original string unchanged if none are present, otherwise returns
// a new string with control bytes encoded as %XX (uppercase hex) and multibyte
// UTF-8 sequences encoded as %XX%XX%XX (one %XX per byte).
func escapeControl(s string) string {
	var hasControl bool
	for i := 0; i < len(s); i++ {
		b := s[i]
		if isControlByte(b) {
			hasControl = true
			break
		}
		// Check for start of UTF-8 multibyte sequence that might be a control char
		if b >= 0xC0 { // Start of a multibyte UTF-8 sequence
			r, size := utf8.DecodeRuneInString(s[i:])
			if isControlRune(r) {
				hasControl = true
				break
			}
			i += size - 1 // Skip to end of this rune
		}
	}

	if !hasControl {
		return s
	}

	// Build the escaped version
	var buf bytes.Buffer
	i := 0
	for i < len(s) {
		b := s[i]
		switch {
		case isControlByte(b):
			buf.WriteString(byteToPercent(b))
			i++
		case b >= 0xC0: // Potential multibyte UTF-8
			r, size := utf8.DecodeRuneInString(s[i:])
			if isControlRune(r) {
				// Encode each byte of the UTF-8 sequence as %XX
				for j := range size {
					buf.WriteString(byteToPercent(s[i+j]))
				}
				i += size
			} else {
				buf.WriteByte(b)
				i++
			}
		default:
			buf.WriteByte(b)
			i++
		}
	}

	return buf.String()
}

// isControlByte returns true for control bytes (0x00–0x1F, 0x7F).
func isControlByte(b byte) bool {
	return (b <= 0x1F) || b == 0x7F
}

// isControlRune returns true for control runes (U+0085, U+2028, U+2029).
func isControlRune(r rune) bool {
	return r == 0x0085 || r == 0x2028 || r == 0x2029
}

// byteToPercent converts a single byte to percent-encoded form (%XX, uppercase).
func byteToPercent(b byte) string {
	const hex = "0123456789ABCDEF"
	return "%" + string(hex[b>>4]) + string(hex[b&0x0F])
}
