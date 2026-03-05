package util

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

type textToken struct {
	start int // byte offset inclusive
	end   int // byte offset exclusive
}

const maxWordPieceRunes = 8

// CountApproxTokens returns the approximate token count for text.
// Tokens are word-like spans and punctuation marks.
func CountApproxTokens(text string) int {
	return len(tokenizeText(text))
}

// ChunkText splits text into token-based chunks with token overlap, trying to
// break at natural boundaries.
//
// chunkSize and overlap are expressed in tokens (not characters).
// Returns the original text as a single chunk if it fits within chunkSize tokens.
// There is no limit on the number of chunks returned; long documents produce as many chunks as needed.
//
// This is used for embedding inputs (store content and long discover queries).
func ChunkText(text string, chunkSize, overlap int) []string {
	if chunkSize <= 0 {
		return []string{text}
	}
	if overlap < 0 {
		overlap = 0
	}
	tokens := tokenizeText(text)
	if len(tokens) <= chunkSize {
		return []string{text}
	}

	var chunks []string
	startTok := 0
	total := len(tokens)

	for startTok < total {
		endTok := startTok + chunkSize
		if endTok > total {
			endTok = total
		}

		if endTok < total {
			if adjusted := findBoundaryEndToken(text, tokens, startTok, endTok); adjusted > startTok {
				endTok = adjusted
			}
		}

		startByte := tokens[startTok].start
		endByte := tokens[endTok-1].end
		chunk := strings.TrimSpace(text[startByte:endByte])
		if chunk != "" {
			chunks = append(chunks, chunk)
		}

		// Move forward, accounting for token overlap.
		nextStartTok := endTok - overlap
		if nextStartTok <= startTok {
			nextStartTok = endTok // Prevent infinite loop
		}
		startTok = nextStartTok
	}

	if len(chunks) == 0 {
		return []string{text}
	}
	return chunks
}

func tokenizeText(text string) []textToken {
	if text == "" {
		return nil
	}
	tokens := make([]textToken, 0, len(text)/4)
	for i := 0; i < len(text); {
		r, size := utf8.DecodeRuneInString(text[i:])
		if unicode.IsSpace(r) {
			i += size
			continue
		}
		start := i
		if isWordRune(r) {
			i += size
			for i < len(text) {
				r2, s2 := utf8.DecodeRuneInString(text[i:])
				if !isWordRune(r2) {
					break
				}
				i += s2
			}
			tokens = append(tokens, splitWordToken(text, start, i)...)
			continue
		}
		// Punctuation/symbol token.
		i += size
		tokens = append(tokens, textToken{start: start, end: i})
	}
	return tokens
}

func splitWordToken(text string, start, end int) []textToken {
	if start >= end {
		return nil
	}
	// Break very long contiguous "word" runs into subword pieces to better
	// approximate modern BPE tokenizers and avoid pathological under-counting.
	pieces := make([]textToken, 0, 1+(end-start)/maxWordPieceRunes)
	pieceStart := start
	runesInPiece := 0
	for i := start; i < end; {
		_, size := utf8.DecodeRuneInString(text[i:])
		runesInPiece++
		i += size
		if runesInPiece >= maxWordPieceRunes {
			pieces = append(pieces, textToken{start: pieceStart, end: i})
			pieceStart = i
			runesInPiece = 0
		}
	}
	if pieceStart < end {
		pieces = append(pieces, textToken{start: pieceStart, end: end})
	}
	return pieces
}

func isWordRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '\''
}

func findBoundaryEndToken(text string, tokens []textToken, startTok, endTok int) int {
	// Search backward from end for natural boundaries, but avoid very short chunks.
	minTok := startTok + (endTok-startTok)/2
	best := -1
	bestScore := -1

	for i := endTok; i > minTok; i-- {
		b := tokens[i-1].end
		score := boundaryScore(text, b)
		if score > bestScore {
			bestScore = score
			best = i
			if score >= 3 {
				break // strong paragraph/sentence boundary
			}
		}
	}
	if best > startTok && bestScore > 0 {
		return best
	}
	return endTok
}

func boundaryScore(text string, boundary int) int {
	if boundary <= 0 || boundary > len(text) {
		return 0
	}
	prefix := text[:boundary]
	trimmed := strings.TrimRight(prefix, " \t\r")
	if strings.HasSuffix(trimmed, "\n\n") {
		return 4
	}
	if strings.HasSuffix(trimmed, ".") || strings.HasSuffix(trimmed, "!") || strings.HasSuffix(trimmed, "?") {
		return 3
	}
	if strings.HasSuffix(trimmed, ";") || strings.HasSuffix(trimmed, ":") {
		return 2
	}
	if strings.HasSuffix(trimmed, ",") {
		return 1
	}
	return 0
}
