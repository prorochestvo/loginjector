package httptap

import (
	"bytes"
	"unicode/utf8"
)

// escapeControl percent-encodes control characters in the input string.
// Control characters include: bytes 0x00–0x1F, 0x7F, 0x80–0x9F; Unicode
// runes U+0085, U+2028, U+2029. Returns the original string unchanged if
// none are present; otherwise returns a new string with each control byte
// encoded as %XX (uppercase hex), one %XX per byte.
func escapeControl(s string) string {
	var hasControl bool
	for i := 0; i < len(s); i++ {
		b := s[i]
		if isControlByte(b) {
			hasControl = true
			break
		}
		// Check for multibyte UTF-8 that might be a control rune
		if b >= 0x80 {
			r, size := utf8.DecodeRuneInString(s[i:])
			if r == utf8.RuneError && size == 1 {
				// Invalid UTF-8 (lone byte >= 0x80): escape the byte
				hasControl = true
				break
			}
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
		case b >= 0x80:
			r, size := utf8.DecodeRuneInString(s[i:])
			if r == utf8.RuneError && size == 1 {
				// Invalid UTF-8: escape the lone byte
				buf.WriteString(byteToPercent(b))
				i++
			} else if isControlRune(r) {
				// Encode each byte of the UTF-8 sequence as %XX
				for j := range size {
					buf.WriteString(byteToPercent(s[i+j]))
				}
				i += size
			} else {
				// Valid non-control UTF-8: keep as-is
				for j := range size {
					buf.WriteByte(s[i+j])
				}
				i += size
			}
		default:
			buf.WriteByte(b)
			i++
		}
	}

	return buf.String()
}

// isControlByte returns true for control bytes: 0x00–0x1F, 0x7F, 0x80–0x9F.
func isControlByte(b byte) bool {
	return (b <= 0x1F) || (b >= 0x7F && b <= 0x9F)
}

// isControlRune returns true for control runes: U+0085, U+2028, U+2029.
// When encoded, each rune is percent-encoded with one %XX per UTF-8 byte.
func isControlRune(r rune) bool {
	return r == 0x0085 || r == 0x2028 || r == 0x2029
}

// byteToPercent converts a single byte to percent-encoded form (%XX, uppercase).
func byteToPercent(b byte) string {
	const hex = "0123456789ABCDEF"
	return "%" + string(hex[b>>4]) + string(hex[b&0x0F])
}
