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

	"github.com/johnknl/replicators"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestHub_SendReceiveAndCancel(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	hub := replicators.NewHub(
		ctx,
		replicators.WithSendBuffer[int](1),
		replicators.WithCounterHandler[int](),
		replicators.WithDeliveryTimeout[int](time.Second),
		replicators.WithAttachBuffer[int](0),
		replicators.WithCancelBuffer[int](0),
	)

	subscriber, err := hub.Subscribe(ctx)
	require.NoError(t, err)

	err = hub.Broadcast(ctx, 1)
	require.NoError(t, err)

	<-subscriber.Data()

	stats := hub.Stats(ctx)

	require.Equal(t, 1, int(stats.Counts.Sent))
	require.Equal(t, 0, int(stats.Counts.Dropped))
	require.Equal(t, 1, int(stats.Counts.Subscriptions))

	require.Equal(t, 1, stats.Gauges.Subscriptions)

	require.ErrorIs(t, subscriber.Cancel(ctx), replicators.ErrSubscriptionCancelled)
	require.ErrorIs(t, subscriber.Err(), replicators.ErrSubscriptionCancelled)

	stats = hub.Stats(ctx)
	require.Equal(t, 0, stats.Gauges.Subscriptions)
	require.Equal(t, 1, int(stats.Counts.Subscriptions))

	select {
	case _, ok := <-subscriber.Data():
		require.False(t, ok, "Expected channel to be closed")
	default:
		require.False(t, true, "Expected channel to be closed")
	}
}

func TestHub_Shutdown(t *testing.T) {
	t.Parallel()
	rootCtx := t.Context()
	producerCtx, producerCancel := context.WithCancel(rootCtx)

	subCount := 10

	hub := replicators.NewHub(
		producerCtx,
		replicators.WithSendBuffer[int](subCount),
		replicators.WithAttachBuffer[int](0),
		replicators.WithDeliveryTimeout[int](time.Second),
	)

	subs := make([]*replicators.Subscription[int], subCount)
	for i := range subCount {
		consumerCtx, consumerCancel := context.WithCancel(rootCtx)
		defer consumerCancel()
		sub, err := hub.Subscribe(consumerCtx)
		require.NoError(t, err)
		subs[i] = sub
	}

	for i := range subCount {
		err := hub.Broadcast(producerCtx, i)
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

// TestDropScenario tests a scenario where a slow consumer is dropped
// due to exceeding the delivery timeout.
//
// It also gathers some crude timings for illustrative purposes:
//
//	Consumer 0 finished after 51.67008903s, worked 51.644435846s, waited 18.713723ms
//	Consumer 1 finished after 51.670075284s, worked 51.49508549s, waited 167.582507ms
//	Consumer 2 finished after 2m15.624082738s, worked 2m15.607502618s, waited 9.464384ms
//
// This illustrates how the consumer right before a slow one spends the most time waiting.
// There is backpressure into the consumer before as well.
func TestDropScenario(t *testing.T) {
	t.Parallel()
	tests := []struct {
		n int // Number of messages to send
	}{
		{n: 10},
		{n: 10000},
	}

	for _, tt := range tests {
		t.Run("n="+strconv.Itoa(tt.n), func(t *testing.T) {
			t.Parallel()

			subDropScenario := scenario{
				name:            t.Name(),
				deliveryTimeout: 10 * time.Millisecond, // The slow consumer takes twice this
				repeats:         tt.n,
				consumers: []*consumer{
					{buffer: 0, workTime: 5 * time.Millisecond},
					{buffer: 0, workTime: 5 * time.Millisecond},
					{buffer: tt.n / 2, workTime: 20 * time.Millisecond}, // Slow consumer with half-buffer size of messages sent
				},
			}
			t.Logf("Running subscriber dropping scenario with %d messages", tt.n)
			stats := runSimpleSubscriberScenario(t, subDropScenario)
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
	name            string
	consumers       []*consumer
	deliveryTimeout time.Duration
	repeats         int // Number of messages to send
}

func runSimpleSubscriberScenario(t *testing.T, scenario scenario) *replicators.Stats {
	t.Helper()
	ctx := t.Context()

	hub := replicators.NewHub[int](ctx,
		replicators.WithAttachBuffer[int](0),
		replicators.WithDeliveryTimeout[int](scenario.deliveryTimeout),
		replicators.WithCounterHandler[int](),
		replicators.WithEventHandlerFunc[int](func(_ context.Context, event replicators.Event[int]) {
			switch e := event.(type) {
			case replicators.EvtSubDropped[int]:
				t.Logf("%s: Subscriber dropped: %#v", scenario.name, e)
			}
		}),
	)

	wg := sync.WaitGroup{}
	start := sync.RWMutex{}
	start.Lock() // Lock the start mutex to ensure all consumers start at the same time

	for consumerIdx, consumer := range scenario.consumers {
		subscription, err := hub.Subscribe(ctx, replicators.WithReceiveBuffer[int](consumer.buffer))
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
					"%s: Consumer %d finished after %v, worked %v, waited %v",
					scenario.name,
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
					err = hub.Cancel(ctx, subscription)
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
		err := hub.Broadcast(ctx, i)
		require.NoError(t, err)
	}

	wg.Wait()

	return hub.Stats(ctx)
}
