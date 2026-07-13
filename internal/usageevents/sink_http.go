// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package usageevents

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// HTTPSink sends usage events to an HTTP endpoint.
type HTTPSink struct {
	client               *http.Client
	url                  string
	headers              map[string]string
	maxResponseBodyBytes int64
}

func NewHTTPSink(client *http.Client, url string, headers map[string]string, maxResponseBodyBytes int) *HTTPSink {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPSink{
		client:               client,
		url:                  url,
		headers:              headers,
		maxResponseBodyBytes: int64(maxResponseBodyBytes),
	}
}

func (s *HTTPSink) Publish(ctx context.Context, event UsageEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal usage event: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("failed to create usage event request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Usage-Event-Id", event.EventID)
	req.Header.Set("X-Usage-Event-Schema-Version", event.SchemaVersion)
	for k, v := range s.headers {
		req.Header.Set(k, v)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send usage event: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusConflict {
		// 409 indicates duplicate event ID for idempotent adapters.
		return nil
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, s.maxResponseBodyBytes))
		return fmt.Errorf("usage event HTTP sink returned status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}
