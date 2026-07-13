// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package usageevents

import (
	"fmt"
	"strings"
)

const (
	ModeBestEffort = "best_effort"
	ModeRequired   = "required"

	SinkNoop = "noop"
	SinkLog  = "log"
	SinkHTTP = "http"

	StreamingModeBestEffortFallback = "best_effort_fallback"

	BackoffPolicyExponential = "exponential"
	BackoffPolicyConstant    = "constant"
)

// Config captures usage event publishing behavior configured through extproc flags.
type Config struct {
	Mode          string
	Sink          string
	TimeoutMs     int
	MaxRetries    int
	BackoffPolicy string

	StreamingMode string

	Attributes map[string]string

	HTTPURL              string
	HTTPHeaders          map[string]string
	HTTPMaxResponseBytes int
}

// Validate checks config constraints independent from specific sink implementations.
func (c Config) Validate() error {
	switch c.Mode {
	case ModeBestEffort, ModeRequired:
	default:
		return fmt.Errorf("invalid usage events mode %q (allowed: %s, %s)", c.Mode, ModeBestEffort, ModeRequired)
	}

	switch c.Sink {
	case SinkNoop, SinkLog, SinkHTTP:
	default:
		return fmt.Errorf("invalid usage events sink %q (allowed: %s, %s, %s)", c.Sink, SinkNoop, SinkLog, SinkHTTP)
	}

	switch c.BackoffPolicy {
	case BackoffPolicyExponential, BackoffPolicyConstant:
	default:
		return fmt.Errorf("invalid usage events backoff policy %q (allowed: %s, %s)", c.BackoffPolicy, BackoffPolicyExponential, BackoffPolicyConstant)
	}

	switch c.StreamingMode {
	case StreamingModeBestEffortFallback:
	default:
		return fmt.Errorf("invalid usage events streaming mode %q (allowed: %s)", c.StreamingMode, StreamingModeBestEffortFallback)
	}

	if c.TimeoutMs <= 0 {
		return fmt.Errorf("usage events timeout must be > 0, got %d", c.TimeoutMs)
	}
	if c.MaxRetries < 0 {
		return fmt.Errorf("usage events max retries must be >= 0, got %d", c.MaxRetries)
	}
	if c.HTTPMaxResponseBytes <= 0 {
		return fmt.Errorf("usage events HTTP max response bytes must be > 0, got %d", c.HTTPMaxResponseBytes)
	}

	if c.Sink == SinkHTTP && strings.TrimSpace(c.HTTPURL) == "" {
		return fmt.Errorf("usage events HTTP URL must be set when sink is %q", SinkHTTP)
	}
	return nil
}

// ParseHTTPHeaderMapping parses comma-separated key-value pairs for HTTP request headers.
// Format is "header1:value1,header2:value2".
func ParseHTTPHeaderMapping(s string) (map[string]string, error) {
	if s == "" {
		return nil, nil
	}
	result := make(map[string]string)
	pairs := strings.Split(s, ",")
	for i, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			return nil, fmt.Errorf("empty HTTP header pair at position %d", i+1)
		}
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid HTTP header pair at position %d: %q (expected format: header:value)", i+1, pair)
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if key == "" || value == "" {
			return nil, fmt.Errorf("empty HTTP header key or value at position %d: %q", i+1, pair)
		}
		result[key] = value
	}
	return result, nil
}
