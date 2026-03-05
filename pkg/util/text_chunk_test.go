package util

import (
	"testing"
)

func TestCountApproxTokens_BasicAndLongWordSplitting(t *testing.T) {
	if got := CountApproxTokens(""); got != 0 {
		t.Fatalf("expected 0 tokens for empty text, got %d", got)
	}

	// maxWordPieceRunes is 8, so 20 runes should split into 3 token pieces.
	if got := CountApproxTokens("abcdefghijklmnopqrst"); got != 3 {
		t.Fatalf("expected long contiguous word to split into 3 tokens, got %d", got)
	}

	if got := CountApproxTokens("hello, world!"); got != 4 {
		t.Fatalf("expected tokens for words and punctuation, got %d", got)
	}
}

func TestChunkText_EdgeCases(t *testing.T) {
	text := "one two three"

	if got := ChunkText(text, 0, 0); len(got) != 1 || got[0] != text {
		t.Fatalf("chunkSize<=0 should return original text, got %#v", got)
	}

	if got := ChunkText(text, 10, -1); len(got) != 1 || got[0] != text {
		t.Fatalf("negative overlap with small text should return original text, got %#v", got)
	}
}

func TestChunkText_BoundaryAndOverlapHandling(t *testing.T) {
	text := "First sentence. Second sentence, with comma. Third sentence!"
	chunks := ChunkText(text, 4, 1)
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %#v", chunks)
	}

	// The first chunk should prefer ending at a sentence boundary.
	first := chunks[0]
	if first[len(first)-1] != '.' && first[len(first)-1] != '!' && first[len(first)-1] != '?' {
		t.Fatalf("expected first chunk to end near sentence boundary, got %q", first)
	}
}

func TestChunkText_OverlapGreaterThanChunkSizeAvoidsInfiniteLoop(t *testing.T) {
	text := "a b c d e f g h i j"
	chunks := ChunkText(text, 2, 5)
	if len(chunks) == 0 {
		t.Fatalf("expected chunks, got none")
	}
	if len(chunks) > 20 {
		t.Fatalf("unexpectedly high chunk count, possible loop: %d", len(chunks))
	}
}

func TestBoundaryScore(t *testing.T) {
	if got := boundaryScore("para1\n\npara2", len("para1\n\n")); got != 4 {
		t.Fatalf("expected paragraph boundary score 4, got %d", got)
	}
	if got := boundaryScore("done.", len("done.")); got != 3 {
		t.Fatalf("expected sentence boundary score 3, got %d", got)
	}
	if got := boundaryScore("k:v;", len("k:v;")); got != 2 {
		t.Fatalf("expected punctuation boundary score 2, got %d", got)
	}
	if got := boundaryScore("a,b", len("a,")); got != 1 {
		t.Fatalf("expected comma boundary score 1, got %d", got)
	}
	if got := boundaryScore("abc", 0); got != 0 {
		t.Fatalf("expected out-of-range boundary score 0, got %d", got)
	}
}
