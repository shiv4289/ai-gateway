# Per-Request AI Usage Event Export

## Table of Contents

<!-- toc -->

- [Summary](#summary)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Background](#background)
- [Design](#design)
  - [Event Flow](#event-flow)
  - [Delivery Modes](#delivery-modes)
  - [Usage Event Schema](#usage-event-schema)
  - [Extension Sections](#extension-sections)
  - [Sink Interface](#sink-interface)
  - [HTTP Sink Contract](#http-sink-contract)
  - [API Design](#api-design)
- [Implementation Plan](#implementation-plan)
- [Risks and Caveats](#risks-and-caveats)
  - [Streaming Responses](#streaming-responses)
  - [Duplicate Events](#duplicate-events)
  - [Backpressure](#backpressure)
  - [Privacy](#privacy)
- [Open Questions for Community Discussion](#open-questions-for-community-discussion)

<!-- /toc -->

## Summary

This proposal introduces a per-request AI usage event export mechanism for Envoy AI Gateway. It defines a `UsageEvent` schema, two delivery modes (`best_effort` and `required`), and a transport-agnostic sink interface.

The goal is to give downstream systems structured per-request usage records over a sink of their choice - from lightweight logging to durable message queues. Envoy AI Gateway emits the event and stops there - it has no opinion on what consumers do with it.

Use `required` mode when every event must reach the downstream system: customer billing, compliance audit trails, and budget enforcement. Use `best_effort` for operational visibility such as dashboards, analytics, and alerting.

The event publisher runs inside the existing extproc binary, which already holds token usage, model, route, and backend metadata at response time. No new sidecar, controller, or processing component is introduced. Configuration is handled through extproc binary flags.

## Goals

- Emit a normalized `UsageEvent` once per AI request whenever usage information is available.
- Support a `best_effort` mode that integrates with existing telemetry-oriented paths.
- Support a `required` mode for workflows where losing an event is worse than failing the request.
- Keep the sink interface transport-agnostic and extensible.
- Keep Envoy AI Gateway free of downstream business logic.
- Keep the schema extensible via the `extensions` map so future features can attach metadata without breaking existing consumers - for example, GPU queue time for self-hosted inference, cache hit/miss for response caching, or which rate limit rule fired for policy decisions.

## Non-Goals

- Events are handed off to the sink with bounded retries; durability beyond that is the sink's responsibility.
- The sink interface is open; a Kafka adapter, OTel exporter, or webhook forwarder are for consumers to build.
- Prompts, responses, and raw API keys must never appear in emitted events.

## Background

Envoy AI Gateway already exposes AI usage data through observability surfaces such as [Prometheus metrics](https://aigateway.envoyproxy.io/docs/capabilities/observability/metrics), [distributed traces](https://aigateway.envoyproxy.io/docs/capabilities/observability/tracing), and [access logs](https://aigateway.envoyproxy.io/docs/capabilities/observability/accesslogs). Those surfaces are the right answer for dashboards, analytics, alerting, debugging, capacity planning, and trend analysis, and they should remain so.

However, those channels are generally best-effort. They are not designed to be a request-path delivery contract.

Some downstream workflows need stronger loss-avoidance behavior for request-level usage records. Examples include:

- **Customer usage reporting** - per-request records for SaaS billing and invoicing.
- **Budget enforcement** - spending controls that must see every request before or immediately after it completes.
- **Provider reconciliation** - comparing gateway-observed token counts with provider invoices.
- **Compliance ledgers** - append-only audit records required by policy or regulation.
- **Internal showback and FinOps allocation** - attributing AI spend to teams, products, or cost centers.
- **Abuse and anomaly detection** - workflows that depend on complete per-request records to detect unusual patterns.

This proposal adds an explicit usage event export path for those workflows while keeping Envoy AI Gateway stateless and vendor-neutral.

## Design

### Event Flow

```text
Client
  |
  v
Envoy (with AI Gateway ExtProc filter)
  |
  |  [request path] model extraction, backend selection, transformation
  |
  v
AI Service Backend (OpenAI, Bedrock, Vertex AI, etc.)
  |
  |  [response path] token usage extraction, response transformation
  |
  v
ExtProc processResponseBody / processResponseTrailers
  |
  +-- build UsageEvent from normalized AI metadata
  |
  +-- [best_effort]  publish through existing telemetry path (non-blocking)
  |
  '-- [required]  hand off to configured sink
                       +-- sink acknowledges -> request completes normally
                       '-- sink fails / times out -> request fails with 500
```

### Delivery Modes

#### `best_effort`

Best-effort mode is suitable for operational use cases: dashboards, analytics, alerting, debugging, trend analysis, and capacity planning.

In this mode, export failures do not affect user traffic. The event is published through telemetry-oriented paths (e.g., an OTel span event or an access log field). This mode should not be documented as appropriate for workflows that require loss-avoidance semantics.

#### `required`

`required` mode is intended for workflows where losing a usage event is worse than rejecting the request - for example, billing or compliance ledgers.

In this mode, Envoy AI Gateway attempts to hand off the event to the configured sink before completing the response. If the sink does not acknowledge within the configured policy (timeout + retries), the request fails.

This does not mean Envoy AI Gateway guarantees end-to-end durability. The sink is responsible for acknowledging only after it has met its own durability requirements (e.g., written to a replicated log).

### Usage Event Schema

The `UsageEvent` has a stable core and optional extension sections. All fields in the stable core are expected to be present for every successfully processed AI request.

```go
// UsageEvent is the normalized per-request AI usage record emitted by the gateway.
type UsageEvent struct {
    // SchemaVersion allows consumers to detect breaking changes.
    SchemaVersion string `json:"schema_version"`

    // EventID is a stable identifier for this event. Used for deduplication across retries.
    EventID string `json:"event_id"`

    // EmittedAt is the Unix timestamp in milliseconds when the event was emitted.
    EmittedAt int64 `json:"emitted_at"`

    Request   UsageEventRequest   `json:"request"`
    Route     UsageEventRoute     `json:"route"`
    Backend   UsageEventBackend   `json:"backend"`
    Model     UsageEventModel     `json:"model"`
    Tokens    UsageEventTokens    `json:"tokens"`
    Latency   UsageEventLatency   `json:"latency"`

    // Attributes contains request-scoped key-value pairs extracted from
    // configured request attributes (e.g. tenant.id, user.id).
    // Keys are allowlisted in UsageEventPolicy.
    Attributes map[string]string `json:"attributes,omitempty"`

    // Extensions carries optional feature-specific metadata.
    // Consumers should ignore unknown extension fields.
    Extensions map[string]json.RawMessage `json:"extensions,omitempty"`
}

type UsageEventRequest struct {
    // ID is the x-request-id or an equivalent stable request identifier.
    ID        string `json:"id"`
    Operation string `json:"operation"` // e.g. "chat", "embedding", "image"
    Status    string `json:"status"`    // "succeeded" | "failed" | "rate_limited"
    // StatusCode is the HTTP status code returned to the client.
    StatusCode int `json:"status_code"`
}

type UsageEventRoute struct {
    Name      string `json:"name"`
    Namespace string `json:"namespace"`
}

type UsageEventBackend struct {
    Name     string `json:"name"`
    Provider string `json:"provider"` // e.g. "openai", "aws_bedrock", "vertex_ai"
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
```

#### Example Payload

```json
{
  "schema_version": "v1",
  "event_id": "evt-7f3a21b0-4c9d-4f2e-b1e3-abc123456789",
  "emitted_at": 1753961025123,
  "request": {
    "id": "req-abc123",
    "operation": "chat",
    "status": "succeeded",
    "status_code": 200
  },
  "route": {
    "name": "llmroute",
    "namespace": "ai-gateway"
  },
  "backend": {
    "name": "openai-primary",
    "provider": "openai"
  },
  "model": {
    "requested": "gpt-4.1",
    "response": "gpt-4.1"
  },
  "tokens": {
    "input": 120,
    "output": 80,
    "total": 200,
    "cached_input": 10
  },
  "latency": {
    "total_ms": 843,
    "time_to_first_token_ms": 210
  },
  "attributes": {
    "tenant.id": "tenant-a",
    "user.id": "user-123"
  }
}
```

### Extension Sections

Optional extensions carry feature-specific metadata and are keyed by a feature name under `extensions`. Consumers must ignore unknown keys.

**Routing extension** (for quota-aware routing):

```json
{
  "extensions": {
    "routing": {
      "strategy": "quota_aware",
      "candidate_backends": ["openai-pt-us-east", "openai-ondemand"],
      "selected_backend": "openai-pt-us-east",
      "fallback_triggered": false
    }
  }
}
```

**Self-hosted inference extension** (for EPP / KServe backends):

```json
{
  "extensions": {
    "self_hosted": {
      "runtime": "vllm",
      "deployment": "llama-prod",
      "gpu_pool": "a100",
      "queue_time_ms": 12,
      "time_to_first_token_ms": 230
    }
  }
}
```

**Cache extension** (for future semantic caching):

```json
{
  "extensions": {
    "cache": {
      "hit": true,
      "backend": "semantic-cache-primary"
    }
  }
}
```

### Sink Interface

The sink interface is intentionally small. It decouples event generation from transport:

```go
// UsageEventSink is implemented by any transport that can receive a UsageEvent.
// Publish must be safe to call concurrently.
type UsageEventSink interface {
    // Publish delivers the event to the sink.
    // In required mode, a non-nil error causes the AI request to fail.
    // In best_effort mode, errors are counted and discarded.
    Publish(ctx context.Context, event UsageEvent) error
}
```

The initial implementation ships three sinks:

| Sink | Description |
|---|---|
| `noop` | Drops all events. Default when usage events are not configured. |
| `log` | Serializes events as JSON and emits via the existing structured logger. Suitable for best-effort mode. |
| `http` | Sends events to an external HTTP endpoint. This enables out-of-repo adapters that forward to JetStream, Kafka, or other systems. |

Additional sinks (e.g., an OTel exporter, a gRPC streaming sink, or a Kafka producer adapter) can be added by implementing `UsageEventSink` without modifying event generation or schema code.

### HTTP Sink Contract

The `http` sink allows Envoy AI Gateway to remain vendor-neutral while integrating with external adapters.

In this model, extproc publishes `UsageEvent` to an HTTP endpoint, and the external adapter is responsible for forwarding the event to vendor-specific systems (for example, NATS JetStream or Kafka).

#### Endpoint

- Method: `POST`
- Path: operator-defined (for example, `/v1/usage-events`)
- Content-Type: `application/json`
- Body: `UsageEvent` JSON

#### Required request headers

- `Content-Type: application/json`
- `X-Usage-Event-Id: <event_id>` (stable idempotency key)
- `X-Usage-Event-Schema-Version: v1`

#### Success semantics

The sink endpoint must return a `2xx` status code only after it has accepted the event according to its own durability policy.

- In `best_effort` mode: non-2xx responses are counted and dropped.
- In `required` mode: non-2xx responses (or timeout/network errors) are treated as publish failures and can fail the AI request.

#### Recommended status code behavior for adapters

- `200` / `201` / `202`: event accepted
- `400`: malformed event (non-retryable)
- `401` / `403`: authentication/authorization failure (non-retryable until config fixed)
- `409`: duplicate event id already accepted (treat as success for idempotency)
- `429`: throttled (retryable)
- `5xx`: server-side failure (retryable)

#### Optional response body

Adapters may return a JSON response for diagnostics:

```json
{
  "accepted": true,
  "event_id": "evt-7f3a21b0-4c9d-4f2e-b1e3-abc123456789",
  "message": "stored durably"
}
```

Response body is informational only; gateway behavior is determined by HTTP status code.

#### Idempotency and duplicates

Retries may result in duplicate delivery attempts. Adapters must use `event_id` (and/or `X-Usage-Event-Id`) to deduplicate safely.

#### Authentication

The initial implementation supports static header-based authentication (for example `Authorization: Bearer ...`) configured via extproc flags. Advanced auth mechanisms are out of scope for this proposal.

### API Design

Usage event export is configured through extproc binary flags.

```bash
# Delivery mode: best_effort (default) or required
--usage-events-mode=best_effort

# Sink type: log, noop, or http
--usage-events-sink=log

# Timeout and retry for required mode (ignored in best_effort)
--usage-events-timeout-ms=500
--usage-events-max-retries=3
--usage-events-backoff-policy=exponential

# Behavior when a streaming request is received with required configured.
# Only "best_effort_fallback" is supported in the initial implementation.
--usage-events-streaming-mode=best_effort_fallback

# Comma-separated list of request headers to extract into UsageEvent.Attributes.
# Format: <header-name>:<attribute-key>
# Headers are allowlisted here to prevent accidental exfiltration of sensitive values.
--usage-events-attributes=x-tenant-id:tenant.id,x-user-id:user.id

# HTTP sink endpoint (required when sink=http)
--usage-events-http-url=http://usage-events-adapter.default.svc.cluster.local/v1/usage-events

# Optional static headers added to HTTP sink requests.
# Format: key1:value1,key2:value2
--usage-events-http-headers=authorization:Bearer <token>,x-api-key:<key>

# Optional maximum HTTP response body bytes to read for diagnostics.
--usage-events-http-max-response-bytes=4096
```

The same flags can be provided as environment variables following the extproc convention (e.g. `USAGE_EVENTS_MODE`, `USAGE_EVENTS_SINK`).

When `--usage-events-sink=http` is selected, `--usage-events-http-url` must be set; otherwise extproc fails startup validation.

## Implementation Plan

1. **Define `UsageEvent` schema** - stable core types and JSON serialization.
2. **Add `UsageEventSink` interface** - with `noop`, `log`, and `http` implementations.
3. **Wire event construction in ExtProc** - build `UsageEvent` from existing normalized metadata in `processResponseBody` / `processResponseTrailers`.
4. **Add best-effort publisher** - calls sink asynchronously; errors counted and discarded.
5. **Add fail-closed publisher** - calls configured sink synchronously with bounded timeout/retry; errors propagate to response path. Restricted to non-streaming requests in the initial implementation (see [Streaming Responses](#streaming-responses)).
6. **Add extproc flags** - `--usage-events-mode`, `--usage-events-sink`, `--usage-events-timeout-ms`, `--usage-events-max-retries`, `--usage-events-backoff-policy`, `--usage-events-streaming-mode`, `--usage-events-attributes`, `--usage-events-http-url`, `--usage-events-http-headers`, `--usage-events-http-max-response-bytes`.
7. **Add observability** - metrics for `usage_events_exported_total`, `usage_events_failed_total`, `usage_event_export_duration_ms`, and `usage_events_dropped_total`.
8. **Add integration tests** - validate event construction, attribute extraction, and delivery behavior for both modes, including an in-process HTTP receiver for sink contract validation.

## Risks and Caveats

### Streaming Responses

For streaming responses, final token counts are only known after end-of-stream, by which time response chunks have already been sent to the client. If the sink is unavailable at that point, the response has already been partially delivered and cannot be rolled back - making `required` semantics unreliable for streaming.

The initial implementation introduces a `--usage-events-streaming-mode` flag that controls behavior when a streaming request is received with `required` configured. The only supported value for now is `best_effort_fallback`: extproc falls back to best-effort delivery and logs a warning. Additional values (e.g., `terminate`) are reserved for future proposals once the tradeoffs are better understood.

Operators running billing or compliance workflows against streaming endpoints should be aware of this limitation and compensate at the application layer - for example, by reconciling gateway-emitted events against provider invoices, treating any request where no event was received within a window as requiring manual review, or restricting `required` to non-streaming endpoints where the guarantee holds fully.

### Duplicate Events

Retries in `required` mode can result in duplicate `Publish` calls. The `event_id` field in `UsageEvent` is a stable identifier across retries for the same request and must be used by downstream consumers for deduplication.

### Backpressure

In `required` mode, a slow or unavailable sink can fail AI requests. This is expected and intentional, but it must be documented clearly. Operators must configure bounded `timeoutMs` and monitor `usage_events_failed_total` before enabling `required` in production.

When using `http` sink in `required` mode, adapter availability and latency become part of the request critical path. Operators should run adapters with high availability and ensure adapter `2xx` responses reflect their intended durability policy.

### Privacy

Usage events must not include prompt text, response text, raw API keys, or other sensitive body content. Attributes extracted from request headers are allowlisted explicitly via the `--usage-events-attributes` flag to prevent accidental exfiltration.

## Open Questions for Community Discussion

1. What is the right telemetry representation for `best_effort` events - an OTel span event, a structured access log field, or a dedicated log line? This affects how existing observability pipelines can consume them without additional configuration.
2. What fields should be required for `event_id` stability across backend failover - only the original `x-request-id`, or a combination with route and backend?
3. When per-route granularity or hot-reload is needed, should the configuration surface extend `AIGatewayRoute` directly, or use a separate policy attachment resource?
