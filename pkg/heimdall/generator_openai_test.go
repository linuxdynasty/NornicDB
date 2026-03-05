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
