// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package usageevents

import "encoding/json"

// UsageEvent is the normalized per-request AI usage record emitted by the gateway.
type UsageEvent struct {
	// SchemaVersion allows consumers to detect breaking changes.
	SchemaVersion string `json:"schema_version"`

	// EventID is a stable identifier for this event. Used for deduplication across retries.
	EventID string `json:"event_id"`

	// EmittedAt is the Unix timestamp in milliseconds when the event was emitted.
	EmittedAt int64 `json:"emitted_at"`

	Request UsageEventRequest `json:"request"`
	Route   UsageEventRoute   `json:"route"`
	Backend UsageEventBackend `json:"backend"`
	Model   UsageEventModel   `json:"model"`
	Tokens  UsageEventTokens  `json:"tokens"`
	Latency UsageEventLatency `json:"latency"`

	// Attributes contains request-scoped key-value pairs extracted from
	// configured request attributes (e.g. tenant.id, user.id).
	Attributes map[string]string `json:"attributes,omitempty"`

	// Extensions carries optional feature-specific metadata.
	// Consumers should ignore unknown extension fields.
	Extensions map[string]json.RawMessage `json:"extensions,omitempty"`
}

type UsageEventRequest struct {
	// ID is the x-request-id or an equivalent stable request identifier.
	ID string `json:"id"`
	// Operation is the operation such as "chat", "embedding", "image".
	Operation string `json:"operation"`
	// Status is one of "succeeded", "failed", or "rate_limited".
	Status string `json:"status"`
	// StatusCode is the HTTP status code returned to the client.
	StatusCode int `json:"status_code"`
}

type UsageEventRoute struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type UsageEventBackend struct {
	Name     string `json:"name"`
	Provider string `json:"provider"`
}

type UsageEventModel struct {
	// Requested is the model name sent by the client.
	Requested string `json:"requested"`
	// Response is the model name returned by the provider (may differ for aliases).
	Response string `json:"response"`
}

type UsageEventTokens struct {
	Input              int `json:"input"`
	Output             int `json:"output"`
	Total              int `json:"total"`
	CachedInput        int `json:"cached_input,omitempty"`
	CacheCreationInput int `json:"cache_creation_input,omitempty"`
	Reasoning          int `json:"reasoning,omitempty"`
}

type UsageEventLatency struct {
	// TotalMs is end-to-end latency in milliseconds, from request received to response complete.
	TotalMs int64 `json:"total_ms"`
	// TimeToFirstTokenMs is populated for streaming responses.
	TimeToFirstTokenMs int64 `json:"time_to_first_token_ms,omitempty"`
}
