// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package usageevents

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Publisher sends usage events according to configured delivery semantics.
type Publisher struct {
	cfg    Config
	sink   UsageEventSink
	logger *slog.Logger
}

func NewPublisher(cfg Config, sink UsageEventSink, logger *slog.Logger) *Publisher {
	return &Publisher{cfg: cfg, sink: sink, logger: logger}
}

func (p *Publisher) Publish(ctx context.Context, event UsageEvent, streaming bool) error {
	if p == nil || p.sink == nil {
		return nil
	}

	// Required mode for streaming currently degrades to best effort.
	if streaming && p.cfg.Mode == ModeRequired && p.cfg.StreamingMode == StreamingModeBestEffortFallback {
		p.publishBestEffort(event)
		if p.logger != nil {
			p.logger.Warn("usage events required mode downgraded for streaming request", "streaming_mode", p.cfg.StreamingMode)
		}
		return nil
	}

	if p.cfg.Mode == ModeRequired {
		return p.publishRequired(ctx, event)
	}
	p.publishBestEffort(event)
	return nil
}

func (p *Publisher) publishBestEffort(event UsageEvent) {
	go func() {
		// Bound best-effort publish time so sink outages do not accumulate stuck goroutines.
		attemptCtx, cancel := context.WithTimeout(context.Background(), time.Duration(p.cfg.TimeoutMs)*time.Millisecond)
		defer cancel()
		if err := p.sink.Publish(attemptCtx, event); err != nil && p.logger != nil {
			p.logger.Warn("best effort usage event publish failed", "error", err)
		}
	}()
}

func (p *Publisher) publishRequired(ctx context.Context, event UsageEvent) error {
	var lastErr error
	attempts := p.cfg.MaxRetries + 1
	for attempt := 0; attempt < attempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, time.Duration(p.cfg.TimeoutMs)*time.Millisecond)
		err := p.sink.Publish(attemptCtx, event)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt == attempts-1 {
			break
		}
		// simple bounded backoff for initial implementation
		backoff := 50 * time.Millisecond
		if p.cfg.BackoffPolicy == BackoffPolicyExponential {
			backoff = backoff * time.Duration(1<<attempt)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("usage event publish canceled: %w", ctx.Err())
		case <-time.After(backoff):
		}
	}
	return fmt.Errorf("usage event publish failed after %d attempt(s): %w", attempts, lastErr)
}
