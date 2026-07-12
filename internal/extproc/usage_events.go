// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package extproc

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/envoyproxy/ai-gateway/internal/internalapi"
	"github.com/envoyproxy/ai-gateway/internal/metrics"
	"github.com/envoyproxy/ai-gateway/internal/usageevents"
)

// UsageEventsPublisher is configured at extproc startup.
var UsageEventsPublisher *usageevents.Publisher

// UsageEventsAttributeMapping is configured at extproc startup.
var UsageEventsAttributeMapping map[string]string

func buildUsageEvent(
	requestHeaders map[string]string,
	responseHeaders map[string]string,
	routeName, backendName, responseModel string,
	costs metrics.TokenUsage,
	metricsRecorder metrics.Metrics,
	requestStart time.Time,
) usageevents.UsageEvent {
	const schemaVersion = "v1"

	statusCode, _ := strconv.Atoi(responseHeaders[":status"])
	status := "failed"
	switch {
	case statusCode == 429:
		status = "rate_limited"
	case statusCode >= 200 && statusCode < 300:
		status = "succeeded"
	}

	input, _ := costs.InputTokens()
	output, _ := costs.OutputTokens()
	total, ok := costs.TotalTokens()
	if !ok {
		total = input + output
	}
	cached, hasCached := costs.CachedInputTokens()
	cacheCreation, hasCacheCreation := costs.CacheCreationInputTokens()
	reasoning, hasReasoning := costs.ReasoningTokens()

	attrs := make(map[string]string)
	for headerName, attrKey := range UsageEventsAttributeMapping {
		if v, exists := requestHeaders[headerName]; exists {
			attrs[attrKey] = v
		}
	}
	if len(attrs) == 0 {
		attrs = nil
	}

	path := requestHeaders[":path"]
	return usageevents.UsageEvent{
		SchemaVersion: schemaVersion,
		EventID:       buildUsageEventID(requestHeaders, routeName, backendName),
		EmittedAt:     time.Now().UnixMilli(),
		Request: usageevents.UsageEventRequest{
			ID:         requestHeaders["x-request-id"],
			Operation:  operationFromPath(path),
			Status:     status,
			StatusCode: statusCode,
		},
		Route: usageevents.UsageEventRoute{
			Name: routeName,
		},
		Backend: usageevents.UsageEventBackend{
			Name:     backendName,
			Provider: backendName,
		},
		Model: usageevents.UsageEventModel{
			Requested: requestHeaders[internalapi.ModelNameHeaderKeyDefault],
			Response:  responseModel,
		},
		Tokens: usageevents.UsageEventTokens{
			Input:              int(input), //nolint:gosec
			Output:             int(output), //nolint:gosec
			Total:              int(total), //nolint:gosec
			CachedInput:        maybeInt(hasCached, cached),
			CacheCreationInput: maybeInt(hasCacheCreation, cacheCreation),
			Reasoning:          maybeInt(hasReasoning, reasoning),
		},
		Latency: usageevents.UsageEventLatency{
			TotalMs:            time.Since(requestStart).Milliseconds(),
			TimeToFirstTokenMs: int64(metricsRecorder.GetTimeToFirstTokenMs()),
		},
		Attributes: attrs,
	}
}

func buildUsageEventID(requestHeaders map[string]string, routeName, backendName string) string {
	baseReqID := requestHeaders["x-request-id"]
	if baseReqID == "" {
		baseReqID = "unknown"
	}
	return fmt.Sprintf("%s|%s|%s", baseReqID, routeName, backendName)
}

func operationFromPath(path string) string {
	switch {
	case path == "":
		return "unknown"
	case containsPath(path, "/v1/chat/completions"):
		return "chat"
	case containsPath(path, "/v1/completions"):
		return "completion"
	case containsPath(path, "/v1/embeddings"):
		return "embeddings"
	case containsPath(path, "/v1/responses"):
		return "responses"
	case containsPath(path, "/v1/messages"):
		return "messages"
	default:
		return "unknown"
	}
}

func containsPath(path, suffix string) bool {
	return strings.Contains(path, suffix)
}

func maybeInt(ok bool, v uint32) int {
	if !ok {
		return 0
	}
	return int(v) //nolint:gosec
}
