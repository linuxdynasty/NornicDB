package cypher

import "strings"

// decodeCypherQuotedString decodes a quoted Cypher string literal.
// It supports both doubled quote escaping (Cypher standard) and the
// backslash escapes already tolerated in several parser paths.
func decodeCypherQuotedString(raw string) (string, bool) {
	if len(raw) < 2 {
		return "", false
	}
	quote := raw[0]
	if (quote != '\'' && quote != '"') || raw[len(raw)-1] != quote {
		return "", false
	}

	inner := raw[1 : len(raw)-1]
	if !strings.ContainsRune(inner, rune(quote)) && !strings.ContainsRune(inner, '\\') {
		return inner, true
	}

	var builder strings.Builder
	builder.Grow(len(inner))
	for i := 0; i < len(inner); i++ {
		ch := inner[i]
		if ch == '\\' && i+1 < len(inner) {
			next := inner[i+1]
			switch next {
			case '\\', '\'', '"':
				builder.WriteByte(next)
				i++
				continue
			}
		}
		if ch == quote && i+1 < len(inner) && inner[i+1] == quote {
			builder.WriteByte(quote)
			i++
			continue
		}
		builder.WriteByte(ch)
	}

	return builder.String(), true
}
