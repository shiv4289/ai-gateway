// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package usageevents

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
)

// LogSink emits usage events through the process logger.
type LogSink struct {
	logger *slog.Logger
}

func NewLogSink(logger *slog.Logger) *LogSink {
	return &LogSink{logger: logger}
}

func (s *LogSink) Publish(_ context.Context, event UsageEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal usage event: %w", err)
	}
	s.logger.Info("usage event published", slog.String("usage_event", string(payload)))
	return nil
}
