// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package extproc

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/envoyproxy/ai-gateway/internal/internalapi"
)

func TestOperationFromPath(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"/v1/chat/completions", "chat"},
		{"/v1/chat/completions?stream=true", "chat"},
		{"/v1/embeddings/", "embeddings"},
		{"/v1/images/generations", "image_generation"},
		{"/cohere/v2/rerank", "rerank"},
		{"", "unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			require.Equal(t, tt.want, operationFromPath(tt.path))
		})
	}
}

func TestBuildUsageEventIDPrefersInternalID(t *testing.T) {
	headers := map[string]string{
		"x-request-id":               "client-id",
		internalapi.EnvoyAIGatewayHeaderPrefix + "internal-req-id": "internal-id",
	}
	got := buildUsageEventID(headers, "route-a", "backend-a")
	require.Equal(t, "internal-id|route-a|backend-a", got)
}

func TestProviderFromBackendName(t *testing.T) {
	require.Equal(t, "openai", providerFromBackendName("openai-primary"))
	require.Equal(t, "aws.bedrock", providerFromBackendName("aws-bedrock-main"))
	require.Equal(t, "unknown", providerFromBackendName("custom-backend"))
}
