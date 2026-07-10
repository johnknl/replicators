// MIT License
//
// Copyright (C) 2025 John Kleijn
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE

package replicators_test

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johnklnl/replicators"
	"github.com/stretchr/testify/require"
)

func TestStream_SendReceiveAndCancel(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	stream := replicators.NewHub(
		ctx,
		replicators.WithSendBuffer[int](1),
		replicators.WithCounterHandler[int](),
		replicators.WithDeliveryTimeout[int](time.Second),
		replicators.WithAttachBuffer[int](0),
		replicators.WithCancelBuffer[int](0),
	)

	subscriber, err := stream.Subscribe(ctx, 0, 0)
	require.NoError(t, err)

	err = stream.Broadcast(ctx, 1)
	require.NoError(t, err)

	<-subscriber.Data()

	stats := stream.Stats(ctx)

	require.Equal(t, 1, int(stats.Counts.Sent))
	require.Equal(t, 0, int(stats.Counts.Dropped))
	require.Equal(t, 1, int(stats.Counts.Subscriptions))

	require.Equal(t, 1, stats.Gauges.Subscriptions)

	require.ErrorIs(t, subscriber.Cancel(ctx), replicators.ErrSubscriptionCancelled)
	require.ErrorIs(t, subscriber.Err(), replicators.ErrSubscriptionCancelled)

	stats = stream.Stats(ctx)
	require.Equal(t, 0, stats.Gauges.Subscriptions)
	require.Equal(t, 1, int(stats.Counts.Subscriptions))

	select {
	case _, ok := <-subscriber.Data():
		require.False(t, ok, "Expected channel to be closed")
	default:
		require.False(t, true, "Expected channel to be closed")
	}
}

func TestStream_Shutdown(t *testing.T) {
	t.Parallel()
	rootCtx := t.Context()
	producerCtx, producerCancel := context.WithCancel(rootCtx)

	subCount := 10

	stream := replicators.NewHub(
		producerCtx,
		replicators.WithSendBuffer[int](subCount),
		replicators.WithAttachBuffer[int](0),
		replicators.WithDeliveryTimeout[int](time.Second),
	)

	subs := make([]*replicators.Subscription[int], subCount)
	for i := range subCount {
		consumerCtx, consumerCancel := context.WithCancel(rootCtx)
		defer consumerCancel()
		sub, err := stream.Subscribe(consumerCtx, 0, 0)
		require.NoError(t, err)
		subs[i] = sub
	}

	for i := range subCount {
		err := stream.Broadcast(producerCtx, i)
		require.NoError(t, err)
	}

	var count atomic.Int32

	wg := sync.WaitGroup{}

	for _, sub := range subs {
		wg.Go(func() {
			for range sub.Data() {
				count.Add(1)
			}
		})
	}

	// cancel producer prematurely
	producerCancel()

	wg.Wait()
	require.Equal(t, subCount*subCount, int(count.Load()))
}

// TestImpactOfSlowConsumer tests the impact of a slow consumer on fast consumers.
// This is NOT a benchmark but rather a rudimentary check. time.Sleep() + time.Since()
// is not a precise way to use small units of time, but it should be good enough for this test.
func TestImpactOfSlowConsumer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		n int // Number of messages to send
	}{
		{n: 10},
		// {n: 10000},
	}

	for _, tt := range tests {
		t.Run("n="+strconv.Itoa(tt.n), func(t *testing.T) {
			t.Parallel()

			// Baseline scenario: all fast consumers
			baselineScenario := scenario{
				deliveryTimeout: time.Second,
				repeats:         tt.n,
				consumers: []*consumer{
					{buffer: tt.n, workTime: time.Millisecond},
					{buffer: tt.n, workTime: time.Millisecond},
					{buffer: tt.n, workTime: time.Millisecond},
				},
			}
			t.Logf("Running baseline scenario with %d messages", tt.n)
			stats := runSimpleSubscriberScenario(t, baselineScenario)
			t.Log(stats)

			// Calculate the average completion time of the fast consumers in the baseline scenario
			var baselineAvg time.Duration
			for _, c := range baselineScenario.consumers {
				baselineAvg += c.completionTime
			}
			baselineAvg /= time.Duration(len(baselineScenario.consumers))

			// Comparison scenario: two fast consumers and one slow consumer, mitigated by a buffer of n
			comparisonScenario := scenario{
				deliveryTimeout: time.Second,
				repeats:         tt.n,
				consumers: []*consumer{
					{buffer: 0, workTime: time.Millisecond},
					{buffer: 0, workTime: time.Millisecond},
					{buffer: tt.n, workTime: 2 * time.Millisecond}, // Slow consumer with buffer of n
				},
			}
			t.Logf("Running comparison scenario with %d messages", tt.n)
			stats = runSimpleSubscriberScenario(t, comparisonScenario)
			t.Log(stats)

			// Calculate the average completion time of the fast consumers in the comparison scenario
			var comparisonAvg time.Duration
			for _, c := range comparisonScenario.consumers[:1] {
				comparisonAvg += c.completionTime
			}
			comparisonAvg /= 2

			// Assert that the presence of a slow consumer does not significantly impact fast consumers
			tolerance := baselineAvg + 1*time.Second // Define a reasonable tolerance
			require.LessOrEqual(t, comparisonAvg, tolerance, "Slow consumer significantly impacted fast consumers")

			// Sanity check: slow consumer should not be considerably slowed down
			tolerance = time.Duration(tt.n) * time.Millisecond * 2 * 15 / 10 // Allow 50% extra time
			require.LessOrEqual(
				t,
				comparisonScenario.consumers[2].completionTime,
				tolerance,
				"Slow consumer was considerably slowed down",
			)

			// Comparison scenario: dropping consumers
			subDropScenario := scenario{
				deliveryTimeout: 2 * time.Millisecond, // The slow consumer takes twice this
				repeats:         tt.n,
				consumers: []*consumer{
					{buffer: 0, workTime: time.Millisecond},
					{buffer: 0, workTime: time.Millisecond},
					{buffer: tt.n / 2, workTime: 4 * time.Millisecond}, // Slow consumer with buffer of n/2
				},
			}
			t.Logf("Running subscriber dropping scenario with %d messages", tt.n)
			stats = runSimpleSubscriberScenario(t, subDropScenario)
			t.Log(stats)

			require.Equal(t, int64(tt.n), stats.Counts.Sent, "All messages should be sent")
			require.Equal(t, int64(1), stats.Counts.Dropped, "Only the slow consumer should have been dropped")
		})
	}
}

type consumer struct {
	repeats        int // Number of messages consumed
	buffer         int
	totalWaitTime  time.Duration // Cumulative time spent waiting for messages
	workTime       time.Duration // Time to simulate message processing
	completionTime time.Duration // Time it took to consume all messages
}

type scenario struct {
	consumers       []*consumer
	deliveryTimeout time.Duration
	repeats         int // Number of messages to send
}

func runSimpleSubscriberScenario(t *testing.T, scenario scenario) *replicators.Stats {
	t.Helper()
	ctx := t.Context()

	stream := replicators.NewHub[int](ctx,
		replicators.WithAttachBuffer[int](0),
		replicators.WithDeliveryTimeout[int](scenario.deliveryTimeout),
		replicators.WithCounterHandler[int](),
		replicators.WithEventHandlerFunc[int](func(_ context.Context, event replicators.Event[int]) {
			switch e := event.(type) {
			case replicators.EvtSubDropped[int]:
				t.Logf("Subscriber dropped: %v", e)
			}
		}),
	)

	wg := sync.WaitGroup{}
	start := sync.RWMutex{}
	start.Lock() // Lock the start mutex to ensure all consumers start at the same time

	for consumerIdx, consumer := range scenario.consumers {
		subscription, err := stream.Subscribe(ctx, consumer.buffer, 0)
		require.NoError(t, err)

		wg.Go(func() {
			totalWorkTime := time.Duration(0)
			totalWaitTime := time.Duration(0)

			start.RLock() // Wait for the start signal
			startTime := time.Now()

			// Set waitStart for the first iteration
			waitStart := time.Now()

			finish := func() {
				consumer.completionTime = time.Since(startTime)
				consumer.totalWaitTime = totalWaitTime
				t.Logf(
					"Consumer %d finished after %v, worked %v, waited %v",
					consumerIdx,
					consumer.completionTime,
					totalWorkTime,
					totalWaitTime,
				)
			}

			for range subscription.Data() {
				totalWaitTime += time.Since(waitStart)

				workStart := time.Now()
				time.Sleep(consumer.workTime) // time.Sleep() always sleeps at least 1ms
				totalWorkTime += time.Since(workStart)

				consumer.repeats++
				if consumer.repeats == scenario.repeats {
					finish()
					err = stream.Cancel(ctx, subscription)
					require.ErrorIs(t, err, replicators.ErrSubscriptionCancelled)
					return
				}

				// Reset waitStart for the next iteration
				waitStart = time.Now()
			}

			finish()
		})
	}

	start.Unlock() // Signal all consumers to start processing

	for i := range scenario.repeats {
		err := stream.Broadcast(ctx, i)
		require.NoError(t, err)
	}

	wg.Wait()

	return stream.Stats(ctx)
}
