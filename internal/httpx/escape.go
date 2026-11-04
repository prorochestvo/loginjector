package httpx

import (
	"strings"
	"unicode/utf8"
)

// EscapeControl percent-encodes, one uppercase %XX per byte, every ASCII
// control byte (0x00-0x1F, 0x7F), every C1 control rune (U+0080-U+009F),
// U+2028, U+2029, and every byte that is not part of valid UTF-8. Other
// input, including valid non-control UTF-8, is kept as-is; a string with
// nothing to escape is returned unchanged without allocating.
func EscapeControl(s string) string {
	var hasControl bool
	for i := 0; i < len(s); i++ {
		b := s[i]
		if isControlByte(b) {
			hasControl = true
			break
		}
		if b >= 0x80 {
			r, size := utf8.DecodeRuneInString(s[i:])
			if r == utf8.RuneError && size == 1 {
				hasControl = true
				break
			}
			if isControlRune(r) {
				hasControl = true
				break
			}
			i += size - 1
		}
	}

	if !hasControl {
		return s
	}

	buf := new(strings.Builder)
	i := 0
	for i < len(s) {
		b := s[i]
		switch {
		case isControlByte(b):
			writePercent(buf, b)
			i++
		case b >= 0x80:
			r, size := utf8.DecodeRuneInString(s[i:])
			switch {
			case r == utf8.RuneError && size == 1:
				writePercent(buf, b)
				i++
			case isControlRune(r):
				for j := range size {
					writePercent(buf, s[i+j])
				}
				i += size
			default:
				buf.WriteString(s[i : i+size])
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

// isControlRune returns true for multi-byte control runes: the C1 controls
// U+0080-U+009F (U+0085 NEL and U+009B CSI among them), U+2028 and U+2029.
// When encoded, each rune is percent-encoded with one %XX per UTF-8 byte.
func isControlRune(r rune) bool {
	return (r >= 0x80 && r <= 0x9F) || r == 0x2028 || r == 0x2029
}

// writePercent appends b to buf in percent-encoded form (%XX, uppercase hex).
func writePercent(buf *strings.Builder, b byte) {
	const hex = "0123456789ABCDEF"
	buf.WriteByte('%')
	buf.WriteByte(hex[b>>4])
	buf.WriteByte(hex[b&0x0F])
}
