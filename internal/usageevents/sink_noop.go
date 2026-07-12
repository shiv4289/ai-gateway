// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package usageevents

import "context"

// NoopSink drops all usage events.
type NoopSink struct{}

func NewNoopSink() *NoopSink {
	return &NoopSink{}
}

func (*NoopSink) Publish(context.Context, UsageEvent) error {
	return nil
}
