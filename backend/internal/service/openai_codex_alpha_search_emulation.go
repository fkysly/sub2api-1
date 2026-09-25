package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/websearch"
	"github.com/gin-gonic/gin"
)

// Codex 独立网页搜索（POST /alpha/search）的自建搜索模拟。
//
// Codex 的 web.run 工具是**客户端执行**的：模型只是调用它，真正发请求的是
// Codex 自己——它向 <base_url>/alpha/search POST 一个 SearchRequest，期待
// 拿回 SearchResponse。Codex 只消费其中的 output 字符串（见
// codex-rs/ext/web-search/src/output.rs，它把 output 原样包成纯文本的
// function_call_output），encrypted_output / results 都不是必需的。
//
// 有 OpenAI 账号的分组仍然把该请求转发给上游（原生搜索，是用户付费买的能力）；
// 这里只服务那些没有此类账号的分组——它们原本一律拿到 404。判断入口在
// handler：非 OpenAI/Composite 分组才会走到这里。

const (
	// alphaSearchMaxQueries 对齐 web.run 工具说明里的上限（一次最多 4 条查询）。
	alphaSearchMaxQueries = 4
	// alphaSearchResultsPerQuery 与 /v1/messages 那条路径取同样的条数。
	alphaSearchResultsPerQuery = webSearchDefaultMaxResults
)

// alphaSearchEmulationRequest 只声明我们真正会用的字段；其余（open/click/
// finance/weather/settings 等）本轮不处理，反序列化时自然忽略。
type alphaSearchEmulationRequest struct {
	Commands struct {
		SearchQuery []struct {
			Q       string   `json:"q"`
			Domains []string `json:"domains"`
		} `json:"search_query"`
	} `json:"commands"`
}

// extractAlphaSearchQueries 取出搜索词，并把可选的域名过滤折成 SearXNG 能识别的
// site: 语法。空查询（模型误调 web.run 时按说明会发空 q）会被丢掉。
func extractAlphaSearchQueries(body []byte) []string {
	var req alphaSearchEmulationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil
	}
	out := make([]string, 0, len(req.Commands.SearchQuery))
	for _, q := range req.Commands.SearchQuery {
		query := strings.TrimSpace(q.Q)
		if query == "" {
			continue
		}
		if len(q.Domains) > 0 {
			parts := make([]string, 0, len(q.Domains))
			for _, d := range q.Domains {
				if d = strings.TrimSpace(d); d != "" {
					parts = append(parts, "site:"+d)
				}
			}
			if len(parts) > 0 {
				query += " (" + strings.Join(parts, " OR ") + ")"
			}
		}
		out = append(out, query)
		if len(out) >= alphaSearchMaxQueries {
			break
		}
	}
	return out
}

// TryEmulateCodexAlphaSearch 用自建搜索服务应答一次 Codex 独立搜索请求。
//
// 返回 false 只表示"这条路径不适用"（全局模拟未开、请求里没有可搜的查询、
// manager 未初始化），调用方应维持原有行为（404）。搜索本身返回 0 条结果不算
// 不适用——那是一次成功的搜索，只是没有命中。
func (s *OpenAIGatewayService) TryEmulateCodexAlphaSearch(ctx context.Context, c *gin.Context, body []byte) bool {
	if s == nil || s.settingService == nil || c == nil {
		return false
	}
	if !s.settingService.IsWebSearchEmulationEnabled(ctx) {
		return false
	}
	mgr := getWebSearchManager()
	if mgr == nil {
		return false
	}
	queries := extractAlphaSearchQueries(body)
	if len(queries) == 0 {
		return false
	}

	var sb strings.Builder
	refIndex := 0
	for _, q := range queries {
		// 刻意不按账号解析代理：能走到这里的分组没有可承接该端点的上游账号，
		// 请求直接从本机出网。（doWebSearch 需要 *Account，传 nil 会在
		// resolveAccountProxyURL 里解引用崩溃，所以这里直接调 manager。）
		resp, providerName, err := mgr.SearchWithBestProvider(ctx, websearch.SearchRequest{
			Query:      q,
			MaxResults: alphaSearchResultsPerQuery,
		})
		if err != nil {
			slog.Warn("codex alpha search emulation: search failed", "query", q, "error", err)
			fmt.Fprintf(&sb, "Results for %q:\n(search failed)\n\n", q)
			continue
		}
		slog.Info("codex alpha search emulation: search completed",
			"provider", providerName, "query", q, "results_count", len(resp.Results))

		fmt.Fprintf(&sb, "Results for %q:\n", q)
		if len(resp.Results) == 0 {
			sb.WriteString("(no results)\n\n")
			continue
		}
		for _, r := range resp.Results {
			fmt.Fprintf(&sb, "[turn0search%d] %s\n%s\n", refIndex, r.Title, r.URL)
			if r.Snippet != "" {
				sb.WriteString(r.Snippet)
				sb.WriteString("\n")
			}
			if r.PageAge != "" {
				fmt.Fprintf(&sb, "page_age: %s\n", r.PageAge)
			}
			sb.WriteString("\n")
			refIndex++
		}
	}

	// SearchResponse 的最小可用形态：Codex 只读 output。
	payload, err := json.Marshal(map[string]any{
		"encrypted_output": nil,
		"output":           sb.String(),
	})
	if err != nil {
		slog.Error("codex alpha search emulation: marshal response", "error", err)
		return false
	}
	c.Data(http.StatusOK, "application/json", payload)
	return true
}
