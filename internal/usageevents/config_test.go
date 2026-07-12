// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package usageevents

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigValidate(t *testing.T) {
	cfg := Config{
		Mode:                 ModeBestEffort,
		Sink:                 SinkLog,
		TimeoutMs:            500,
		MaxRetries:           3,
		BackoffPolicy:        BackoffPolicyExponential,
		StreamingMode:        StreamingModeBestEffortFallback,
		HTTPMaxResponseBytes: 4096,
	}
	require.NoError(t, cfg.Validate())

	cfg.Sink = SinkHTTP
	cfg.HTTPURL = "http://adapter.default.svc/v1/usage-events"
	require.NoError(t, cfg.Validate())
}

func TestParseHTTPHeaderMapping(t *testing.T) {
	headers, err := ParseHTTPHeaderMapping("authorization:Bearer test,x-api-key:abc123")
	require.NoError(t, err)
	require.Equal(t, "Bearer test", headers["authorization"])
	require.Equal(t, "abc123", headers["x-api-key"])
}
