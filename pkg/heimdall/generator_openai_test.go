package heimdall

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
)

func TestEstimateToolRoundMessagesTokens(t *testing.T) {
	msgs := []ToolRoundMessage{
		{Role: "system", Content: "You are helpful."},
		{Role: "user", Content: "Hello"},
	}
	n := EstimateToolRoundMessagesTokens(msgs)
	assert.Greater(t, n, 0)
	assert.Less(t, n, 100)
}

func TestTrimMessagesForContext(t *testing.T) {
	// Under limit: unchanged
	msgs := []ToolRoundMessage{
		{Role: "system", Content: "Sys"},
		{Role: "user", Content: "User"},
	}
	out := trimMessagesForContext(msgs, 100000)
	assert.Equal(t, msgs, out)

	// Over limit: drops oldest round(s), keeps system + user
	msgs = make([]ToolRoundMessage, 0, 10)
	msgs = append(msgs, ToolRoundMessage{Role: "system", Content: "S"})
	msgs = append(msgs, ToolRoundMessage{Role: "user", Content: "U"})
	big := strings.Repeat("x", 100000) // ~25K tokens each
	msgs = append(msgs, ToolRoundMessage{Role: "assistant", Content: "ok", ToolCalls: []ParsedToolCall{{Id: "1", Name: "f", Arguments: "{}"}}})
	msgs = append(msgs, ToolRoundMessage{Role: "tool", ToolCallID: "1", Content: big})
	msgs = append(msgs, ToolRoundMessage{Role: "assistant", Content: "ok", ToolCalls: []ParsedToolCall{{Id: "2", Name: "f", Arguments: "{}"}}})
	msgs = append(msgs, ToolRoundMessage{Role: "tool", ToolCallID: "2", Content: big})
	out = trimMessagesForContext(msgs, 30000)                          // ~30K budget
	assert.LessOrEqual(t, EstimateToolRoundMessagesTokens(out), 35000) // allow some slack
	assert.Equal(t, "system", out[0].Role)
	assert.Equal(t, "user", out[1].Role)
}

func TestLooksLikeLocalModel(t *testing.T) {
	tests := []struct {
		model string
		want  bool
	}{
		{"", true},           // empty is treated as local (no model to send)
		{"  ", true},         // whitespace-only
		{"model.gguf", true}, // GGUF file
		{"path/to/model.GGUF", true},
		{"gpt-4o-mini", false},
		{"claude-3-opus", false},
		{"llama-3.1", false},
	}
	for _, tt := range tests {
		t.Run(tt.model, func(t *testing.T) {
			assert.Equal(t, tt.want, looksLikeLocalModel(tt.model))
		})
	}
}

func TestTruncateContentToTokenEstimate(t *testing.T) {
	t.Run("maxTokens zero returns empty", func(t *testing.T) {
		assert.Equal(t, "", truncateContentToTokenEstimate("some content", 0))
	})

	t.Run("maxTokens negative returns empty", func(t *testing.T) {
		assert.Equal(t, "", truncateContentToTokenEstimate("some content", -1))
	})

	t.Run("content within limit is unchanged", func(t *testing.T) {
		content := "short text"
		assert.Equal(t, content, truncateContentToTokenEstimate(content, 1000))
	})

	t.Run("content exceeding limit is truncated", func(t *testing.T) {
		content := strings.Repeat("a", 1000)
		result := truncateContentToTokenEstimate(content, 10) // 10 tokens ≈ 40 chars
		assert.Less(t, len(result), len(content))
		assert.Contains(t, result, "[Truncated for context limit.]")
		assert.True(t, utf8.ValidString(result))
	})

	t.Run("truncation invokes rune boundary backup path", func(t *testing.T) {
		// Exercise mid-rune cut handling.
		// "é" is 2 bytes (0xC3 0xA9). With maxTokens=1 -> maxChars=4, and
		// content is 6 bytes (3 runes), content[:4] ends mid-rune.
		content := "ééé"                                     // 6 bytes
		result := truncateContentToTokenEstimate(content, 1) // maxChars=4
		assert.Contains(t, result, "[Truncated for context limit.]")
		assert.True(t, utf8.ValidString(result), "truncated content must remain valid UTF-8")
	})
}

func TestTruncateForOpenAI(t *testing.T) {
	// Under limit: unchanged
	short := "hello"
	assert.Equal(t, short, truncateForOpenAI(short))

	// At limit: unchanged
	atLimit := strings.Repeat("a", openAIMaxContentPerMessage)
	out := truncateForOpenAI(atLimit)
	assert.Equal(t, atLimit, out)
	assert.True(t, utf8.ValidString(out))

	// Over limit: truncated with suffix, valid UTF-8, under limit
	over := strings.Repeat("x", openAIMaxContentPerMessage+1000)
	out = truncateForOpenAI(over)
	assert.LessOrEqual(t, len(out), openAIMaxContentPerMessage)
	assert.True(t, utf8.ValidString(out))
	assert.Contains(t, out, "[Content truncated for API limit;")
}
