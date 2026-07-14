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
	"errors"
	"fmt"
	"time"
)

const (
	// DefaultSendBuffer is the default size of the send buffer.
	DefaultSendBuffer = 10

	// DefaultAttachBuffer is the default size of the attach buffer.
	DefaultAttachBuffer = 1

	// DefaultCancelBuffer is the default size of the cancel buffer.
	DefaultCancelBuffer = 1

	// DefaultDeliveryTimeout is the default timeout for delivering messages to subscribers.
	DefaultDeliveryTimeout = 100 * time.Millisecond

	// DefaultShutdownTimeout is the default timeout for shutting down the hub.
	DefaultShutdownTimeout = 10 * time.Second
)

var (
	// ErrSubscriptionDropped is returned when a subscription is dropped.
	// This happens when the subscriber is too slow to process messages and the tolerance is exceeded.
	ErrSubscriptionDropped = errors.New("subscription dropped")

	// ErrSubscriptionCancelled is returned when a subscription is cancelled.
	// This happens when the subscriber cancels the subscription.
	ErrSubscriptionCancelled = errors.New("subscription cancelled")
)

// Hub provides the frontend for interacting with the loop.
// It provides an API for sending messages, subscribing to messages, and cancelling subscriptions.
type Hub[T any] struct {
	events EventHandler[T]

	messages chan T
	attach   chan *Subscription[T]
	cancel   chan *Subscription[T]

	counters *counters[T]
	gauges   chan *Gauges
	sample   chan struct{}
	echo     T

	subscriptions   []*Subscription[T]
	deliveryTimeout time.Duration
	shutdownTimeout time.Duration

	echoEnabled bool
}

// NewHub creates a new sub.
func NewHub[T any](ctx context.Context, opts ...func(*Hub[T])) *Hub[T] {
	hub := &Hub[T]{
		subscriptions:   make([]*Subscription[T], 0),
		gauges:          make(chan *Gauges),
		sample:          make(chan struct{}),
		deliveryTimeout: DefaultDeliveryTimeout,
		shutdownTimeout: DefaultShutdownTimeout,
	}

	for _, modifier := range opts {
		modifier(hub)
	}

	if hub.messages == nil {
		hub.messages = make(chan T, DefaultSendBuffer)
	}

	if hub.attach == nil {
		hub.attach = make(chan *Subscription[T], DefaultAttachBuffer)
	}

	if hub.cancel == nil {
		hub.cancel = make(chan *Subscription[T], DefaultCancelBuffer)
	}

	go hub.main(ctx)

	return hub

}

// Broadcast a message to subscribers.
// If the context is canceled, this is effectively a no-op.
func (s *Hub[T]) Broadcast(ctx context.Context, msg T) error {
	select {
	case <-ctx.Done():
		if s.events != nil {
			s.events.HandleEvent(ctx, EvtSendContextCancelled[T]{Msg: msg})
		}

		return ctx.Err()
	case s.messages <- msg:
		if s.events != nil {
			s.events.HandleEvent(ctx, EvtSent[T]{Msg: msg})
		}
		return nil
	}
}

// Cancel a subscription.
// If the context is canceled, cancellation will fail.
func (s *Hub[T]) Cancel(ctx context.Context, sub *Subscription[T]) error {
	select {
	case <-ctx.Done():
		err := fmt.Errorf("cancel failed: %w", ctx.Err())
		if s.events != nil {
			s.events.HandleEvent(ctx, EvtCancelFailed[T]{Sub: sub, Err: err})
		}
		return err
	case s.cancel <- sub:
		sub.setErr(ErrSubscriptionCancelled)
		sub.close()
		return ErrSubscriptionCancelled
	}
}

// Subscribe to messages.
//
// When dealing with a subscriber that has very variable message processing
// time, a larger buffer will help smooth things out. Otherwise, a buffer
// of 1 is usually sufficient. The tolerance parameter is the number of messages
// that can be dropped before the subscription is cancelled. If tolerance is equal
// or lower than 0, the subscription will be cancelled immediately when a message
// is dropped.
//
// The passed context is used to cancel the subscription. If the context is canceled,
// the subscription will be cancelled and an error will be returned. Note that the
// context is not the only way a subscription may be cancelled. If the subscriber is
// too slow to process messages and the tolerance is exceeded, the subscription will
// be cancelled in the main dispatch loop. In that case, the subscription's error will
// be set to ErrSubscriptionDropped. When the context is canceled, the subscription's
// error will be set to context.Context.Err().
func (s *Hub[T]) Subscribe(ctx context.Context, opts ...func(*Subscription[T])) (*Subscription[T], error) {
	sub := newSubscription(ctx, s, opts...)
	select {
	case <-ctx.Done():
		sub.close()
		err := fmt.Errorf("subscribe failed: %w", ctx.Err())
		sub.setErr(err)
		return nil, err
	case s.attach <- sub:
	}

	return sub, nil
}

// Stats returns a snapshot of hub statistics.
// When no CounterHandler is attached, the counts will be zero.
// Note that calling this method will block the main dispatch
// loop briefly while gathering metrics.
//
// The SnapshottingGuages event is emitted at the start
// of the stats gathering process.
//
// Calling this method after shutdown may result in a deadlock
// if the passed context is not canceled.
func (s *Hub[T]) Stats(ctx context.Context) *Stats {
	return s.stats(ctx)
}

// WithEventHandlerFunc adds an EventHandler for the hub.
func WithEventHandlerFunc[T any](fn func(context.Context, Event[T])) func(*Hub[T]) {
	return WithEventHandler(EventHandlerFunc[T](fn))
}

// WithEventHandler adds an EventHandler for the hub.
// It will wrap any existing EventHandler.To set all event handlers,
// use WithEventHandlers instead.
func WithEventHandler[T any](handler EventHandler[T]) func(*Hub[T]) {
	return func(s *Hub[T]) {
		if s.events == nil {
			s.events = handler
			return
		}

		oldHandler := s.events

		s.events = EventHandlerFunc[T](func(ctx context.Context, e Event[T]) {
			oldHandler.HandleEvent(ctx, e)
			handler.HandleEvent(ctx, e)
		})
	}
}

// WithEventHandlers sets the EventHandler(s) for the hub.
func WithEventHandlers[T any](handlers ...EventHandler[T]) func(*Hub[T]) {
	if len(handlers) > 1 {
		return func(s *Hub[T]) {
			s.events = EventHandlerFunc[T](func(ctx context.Context, e Event[T]) {
				for _, h := range handlers {
					h.HandleEvent(ctx, e)
				}
			})
		}
	}

	return func(s *Hub[T]) {
		s.events = handlers[0]
	}
}

// WithSendBuffer sets the buffer size for the send channel.
func WithSendBuffer[T any](size int) func(*Hub[T]) {
	return func(s *Hub[T]) {
		s.messages = make(chan T, size)
	}
}

// WithAttachBuffer sets the buffer size for the attach channel.
// The attach channel is used to queue new subscribers.
// Note that on shutdown, the attach buffer is discarded.
func WithAttachBuffer[T any](size int) func(*Hub[T]) {
	return func(s *Hub[T]) {
		s.attach = make(chan *Subscription[T], size)
	}
}

// WithCancelBuffer sets the buffer size for the cancel channel.
// The cancel channel is used to cancel subscriptions.
// Note that on shutdown, the cancel buffer is discarded.
func WithCancelBuffer[T any](size int) func(*Hub[T]) {
	return func(s *Hub[T]) {
		s.cancel = make(chan *Subscription[T], size)
	}
}

// WithDeliveryTimeout sets the maximum time to wait for a subscriber to process a message.
func WithDeliveryTimeout[T any](timeout time.Duration) func(*Hub[T]) {
	return func(s *Hub[T]) {
		s.deliveryTimeout = timeout
	}
}

// WithShutdownTimeout sets the maximum total time to wait for the hub to shutdown gracefully.
func WithShutdownTimeout[T any](timeout time.Duration) func(*Hub[T]) {
	return func(s *Hub[T]) {
		s.shutdownTimeout = timeout
	}
}

// WithEchoEnabled enables the echo feature.
// When enabled, the hub will echo the last message sent to new subscribers.
func WithEchoEnabled[T any]() func(*Hub[T]) {
	return func(s *Hub[T]) {
		s.echoEnabled = true
	}
}
