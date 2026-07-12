// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package usageevents

import "context"

// UsageEventSink is implemented by any transport that can receive a UsageEvent.
// Publish must be safe to call concurrently.
type UsageEventSink interface {
	// Publish delivers the event to the sink.
	Publish(ctx context.Context, event UsageEvent) error
}
