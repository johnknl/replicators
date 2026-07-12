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

package replicators

import (
	"context"
	"fmt"
	"sync/atomic"
)

// Stats contains the current stats for the stream.
type Stats struct {
	// Gauges contains the current gauges for the stream.
	Gauges *Gauges

	// Counts contains the current counts for the stream.
	Counts *Counts
}

func (s *Stats) String() string {
	return fmt.Sprintf("Stats{Gauges:%+v Counts:%+v}", s.Gauges, s.Counts)
}

// Gauges contains the current gauges for the stream.
type Gauges struct {
	// Queued is the current size of the message queue.
	Queued int

	// Subscriptions is the current number of subscribers
	// attached to the stream.
	Subscriptions int

	// Waiting is the current number of subscribers waiting
	// to be attached to the stream.
	Waiting int

	// Cancelling is the current number of subscribers waiting
	// to be detached from the stream.
	Cancelling int

	// Buffered is the current total number of messages buffered
	// in the delivery buffers. This is the sum of all messages
	// buffered for all subscribers.
	Buffered int
}

// Counts contains the current stats for the stream.
type Counts struct {
	// Subscriptions is the total number of subscriptions created.
	Subscriptions int64

	// Cancellations is the total number of subscriptions cancelled.
	Cancellations int64

	// Sent is the total number of messages as counted when pushed on the queue.
	Sent int64

	// Dropped is the total number of copies/messages dropped. The count is somewhat ambiguous
	// because a message dropped due to lack of subscribers is counted once, but for a single
	// message sent, many copies may be counted if more subscribers are slow.
	Dropped int64

	// Delivered is the total number of copies delivered.
	Delivered int64

	// Undeliverable is the total number of messages that could not be delivered to any subscriber.
	Undeliverable int64
}

type counters[T any] struct {
	subscriptions atomic.Int64
	cancellations atomic.Int64
	sent          atomic.Int64
	undeliverable atomic.Int64
	dropped       atomic.Int64
	delivered     atomic.Int64
}

func (s *counters[T]) snap() *Counts {
	return &Counts{
		Subscriptions: s.subscriptions.Load(),
		Cancellations: s.cancellations.Load(),
		Sent:          s.sent.Load(),
		Dropped:       s.dropped.Load(),
		Delivered:     s.delivered.Load(),
		Undeliverable: s.undeliverable.Load(),
	}
}

var _ EventHandler[any] = &CounterHandler[any]{}

// WithCounterHandler adds a CounterHandler to the stream.
func WithCounterHandler[T any]() func(*Hub[T]) {
	return func(s *Hub[T]) {
		handler := NewCounterHandler[T]()
		s.counters = handler.counters

		WithEventHandler[T](handler)(s)
	}
}

// CounterHandler is an EventHandler that updates the counters based on the events received.
type CounterHandler[T any] struct {
	counters *counters[T]
}

// NewCounterHandler creates a new CounterHandler.
func NewCounterHandler[T any]() *CounterHandler[T] {
	return &CounterHandler[T]{
		counters: &counters[T]{},
	}
}

// HandleEvent updates the stats based on the event type.
func (s *CounterHandler[T]) HandleEvent(_ context.Context, e Event[T]) {
	switch e.(type) {
	case EvtDelivered[T]:
		s.counters.delivered.Add(1)
	case EvtDeliveryTimeout[T]:
		s.counters.dropped.Add(1)
	case EvtNoSubscribers[T], EvtSendContextCancelled[T]:
		s.counters.undeliverable.Add(1)
	case EvtCancelled[T], EvtSubDropped[T]:
		s.counters.cancellations.Add(1)
	case EvtSubscribed[T]:
		s.counters.subscriptions.Add(1)
	case EvtSent[T]:
		s.counters.sent.Add(1)
	}
}
