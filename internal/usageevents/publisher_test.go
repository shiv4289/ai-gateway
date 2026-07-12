// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package usageevents

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeSink struct {
	err      error
	calls    atomic.Int32
	failForN int32
}

func (f *fakeSink) Publish(context.Context, UsageEvent) error {
	cur := f.calls.Add(1)
	if cur <= f.failForN {
		return f.err
	}
	return nil
}

type blockingSink struct {
	calls atomic.Int32
}

func (b *blockingSink) Publish(ctx context.Context, _ UsageEvent) error {
	b.calls.Add(1)
	<-ctx.Done()
	return ctx.Err()
}

func TestPublisherPublishRequired(t *testing.T) {
	sink := &fakeSink{err: errors.New("boom"), failForN: 1}
	p := NewPublisher(Config{
		Mode:                 ModeRequired,
		Sink:                 SinkLog,
		TimeoutMs:            100,
		MaxRetries:           1,
		BackoffPolicy:        BackoffPolicyConstant,
		StreamingMode:        StreamingModeBestEffortFallback,
		HTTPMaxResponseBytes: 1024,
	}, sink, slog.Default())

	err := p.Publish(t.Context(), UsageEvent{EventID: "evt-1", SchemaVersion: "v1"}, false)
	require.NoError(t, err)
	require.EqualValues(t, 2, sink.calls.Load())
}

func TestPublisherPublishStreamingFallback(t *testing.T) {
	sink := &fakeSink{err: errors.New("boom"), failForN: 100}
	p := NewPublisher(Config{
		Mode:                 ModeRequired,
		Sink:                 SinkLog,
		TimeoutMs:            100,
		MaxRetries:           0,
		BackoffPolicy:        BackoffPolicyConstant,
		StreamingMode:        StreamingModeBestEffortFallback,
		HTTPMaxResponseBytes: 1024,
	}, sink, slog.Default())

	err := p.Publish(t.Context(), UsageEvent{EventID: "evt-1", SchemaVersion: "v1"}, true)
	require.NoError(t, err)

	// async best effort path should still invoke sink.
	require.Eventually(t, func() bool {
		return sink.calls.Load() > 0
	}, time.Second, 10*time.Millisecond)
}

func TestPublisherBestEffortUsesTimeout(t *testing.T) {
	sink := &blockingSink{}
	p := NewPublisher(Config{
		Mode:                 ModeBestEffort,
		Sink:                 SinkLog,
		TimeoutMs:            20,
		MaxRetries:           0,
		BackoffPolicy:        BackoffPolicyConstant,
		StreamingMode:        StreamingModeBestEffortFallback,
		HTTPMaxResponseBytes: 1024,
	}, sink, slog.Default())

	require.NoError(t, p.Publish(t.Context(), UsageEvent{EventID: "evt-timeout", SchemaVersion: "v1"}, false))
	require.Eventually(t, func() bool {
		return sink.calls.Load() > 0
	}, 500*time.Millisecond, 10*time.Millisecond)
}
