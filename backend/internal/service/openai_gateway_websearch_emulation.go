package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/websearch"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
)

// Web search emulation for the OpenAI-gateway /v1/messages entry point.
//
// The original emulation (gateway_websearch_emulation.go) hangs off
// GatewayService.Forward, which nothing calls any more: /v1/messages is served
// by OpenAIGatewayService.ForwardAsAnthropic. Without this file the feature is
// unreachable for Messages clients, however it is configured.
//
// The trigger is deliberately narrow — the request must carry exactly ONE tool
// and it must be a web search tool (see isOnlyWebSearchToolInBody). Claude Code
// issues that shape only for its dedicated search sub-request; ordinary turns
// carry dozens of tools and never match, so this intercept cannot alter normal
// traffic. The search/serialisation helpers below are shared with the original
// implementation; only the returned result type differs.

// shouldEmulateWebSearchForOpenAI mirrors GatewayService.shouldEmulateWebSearch.
//
// Judgment chain: manager exists → only web_search tool → global enabled →
// account/channel enabled. Account mode "enabled" short-circuits before the
// channel lookup, so an account can opt in without any channel being defined.
func (s *OpenAIGatewayService) shouldEmulateWebSearchForOpenAI(
	ctx context.Context, account *Account, groupID int64, body []byte,
) bool {
	if getWebSearchManager() == nil {
		return false
	}
	if !isOnlyWebSearchToolInBody(body) {
		return false
	}
	if s.settingService == nil || !s.settingService.IsWebSearchEmulationEnabled(ctx) {
		return false
	}

	switch account.GetWebSearchEmulationMode() {
	case WebSearchModeEnabled:
		return true
	case WebSearchModeDisabled:
		return false
	default: // "default" → follow the channel's per-platform flag
		if groupID <= 0 || s.channelService == nil {
			return false
		}
		ch, err := s.channelService.GetChannelForGroup(ctx, groupID)
		if err != nil || ch == nil {
			return false
		}
		return ch.IsWebSearchEmulationEnabled(account.Platform)
	}
}

// handleWebSearchEmulationForOpenAI resolves the query, runs it against the
// configured search provider and writes the Anthropic Messages response
// (streaming or not) straight to the client, exactly as the original
// implementation does.
func (s *OpenAIGatewayService) handleWebSearchEmulationForOpenAI(
	ctx context.Context, c *gin.Context, account *Account, body []byte, model string,
) (*OpenAIForwardResult, error) {
	startTime := time.Now()

	query := extractSearchQueryFromBody(body)
	if query == "" {
		return nil, fmt.Errorf("web search emulation: no query found in messages")
	}
	if model == "" {
		model = gjson.GetBytes(body, "model").String()
	}
	if model == "" {
		model = defaultWebSearchModel
	}
	stream := gjson.GetBytes(body, "stream").Bool()

	slog.Info("web search emulation: executing search",
		"account_id", account.ID, "account_name", account.Name,
		"query", query, "stream", stream)

	resp, providerName, err := doWebSearch(ctx, account, query)
	if err != nil {
		// Proxy unavailable → surface as a failover so the scheduler can pick
		// another account, matching the original implementation's behaviour.
		if errors.Is(err, websearch.ErrProxyUnavailable) {
			return nil, &UpstreamFailoverError{
				StatusCode:   http.StatusBadGateway,
				ResponseBody: []byte(err.Error()),
			}
		}
		return nil, err
	}

	slog.Info("web search emulation: search completed",
		"provider", providerName, "results_count", len(resp.Results))

	msgID := webSearchMsgIDPrefix + uuid.New().String()
	toolUseID := webSearchToolUseIDPrefix + uuid.New().String()[:16]
	textSummary := buildTextSummary(query, resp.Results)

	if stream {
		if err := writeWebSearchStreamForOpenAI(c, msgID, toolUseID, query, resp, model, textSummary); err != nil {
			return nil, err
		}
	} else {
		if err := writeWebSearchNonStreamForOpenAI(c, msgID, toolUseID, query, resp, model, textSummary); err != nil {
			return nil, err
		}
	}

	return &OpenAIForwardResult{
		RequestID:    msgID,
		ResponseID:   msgID,
		Model:        model,
		BillingModel: model,
		Stream:       stream,
		Duration:     time.Since(startTime),
		Usage: OpenAIUsage{
			InputTokens:  0,
			OutputTokens: len(textSummary) / tokenEstimateDivisor,
		},
	}, nil
}

// writeWebSearchStreamForOpenAI emits the same event sequence as
// writeWebSearchStreamResponse: the fabricated server-side tool call, its
// result, and a text summary the client can render.
func writeWebSearchStreamForOpenAI(
	c *gin.Context, msgID, toolUseID, query string,
	resp *websearch.SearchResponse, model, textSummary string,
) error {
	setSSEHeaders(c)
	w := c.Writer
	for _, fn := range []func() error{
		func() error { return writeSSEMessageStart(w, msgID, model) },
		func() error { return writeSSEServerToolUse(w, toolUseID, query, 0) },
		func() error { return writeSSEToolResult(w, toolUseID, resp.Results, 1) },
		func() error { return writeSSETextBlock(w, textSummary, 2) },
		func() error { return writeSSEMessageEnd(w, len(textSummary)/tokenEstimateDivisor) },
	} {
		if err := fn(); err != nil {
			slog.Warn("web search emulation: SSE write failed, stopping", "error", err)
			return err
		}
	}
	w.Flush()
	return nil
}

// writeWebSearchNonStreamForOpenAI emits the single-JSON-message form.
func writeWebSearchNonStreamForOpenAI(
	c *gin.Context, msgID, toolUseID, query string,
	resp *websearch.SearchResponse, model, textSummary string,
) error {
	msg := map[string]any{
		"id": msgID, "type": "message", "role": "assistant", "model": model,
		"content": []any{
			map[string]any{
				"type": "server_tool_use", "id": toolUseID,
				"name": toolNameWebSearch, "input": map[string]string{"query": query},
			},
			map[string]any{
				"type": "web_search_tool_result", "tool_use_id": toolUseID,
				"content": buildSearchResultBlocks(resp.Results),
			},
			map[string]any{"type": "text", "text": textSummary},
		},
		"stop_reason": "end_turn", "stop_sequence": nil,
		"usage": map[string]int{"input_tokens": 0, "output_tokens": len(textSummary) / tokenEstimateDivisor},
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("web search emulation: marshal response: %w", err)
	}
	c.Data(http.StatusOK, "application/json", body)
	return nil
}
