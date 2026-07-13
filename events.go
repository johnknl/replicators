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
	"time"
)

// EventName is a string that represents the name of an event.
type EventName string

// Event is a marker interface for events that can be handled by an EventHandler.
type Event[T any] interface {
	Name() EventName
}

// EvtDrainCancelled is emitted when draining the send buffer on shutdown
// is cancelled due to the context being cancelled, and contains the number
// of messages left in the queue.
type EvtDrainCancelled struct{ Remaining int }

// Name returns the name of the event.
func (e EvtDrainCancelled) Name() EventName {
	return "drain cancelled"
}

// EvtDraining is emitted when the hub is draining, and contains the number of messages left in the queue.
type EvtDraining struct{ Count int }

// Name returns the name of the event.
func (e EvtDraining) Name() EventName {
	return "draining queue"
}

// EvtShutdown is emitted when the hub is shutting down.
type EvtShutdown struct{}

// Name returns the name of the event.
func (e EvtShutdown) Name() EventName {
	return "shutdown"
}

// EvtSubDropped is emitted when a subscription is dropped due its consumer being slower than allowed.
type EvtSubDropped[T any] struct{ Sub *Subscription[T] }

// Name returns the name of the event.
func (e EvtSubDropped[T]) Name() EventName {
	return "subscription dropped"
}

// EvtDeliveryTimeout is emitted when a message delivery to a subscriber times out.
type EvtDeliveryTimeout[T any] struct {
	Msg     T
	Timeout time.Duration
}

// Name returns the name of the event.
func (e EvtDeliveryTimeout[T]) Name() EventName {
	return "delivery timeout"
}

// Message returns the message that timed out.
func (e EvtDeliveryTimeout[T]) Message() T {
	return e.Msg
}

// SnapshottingGuages is emitted when the hub is snapshotting its gauges.
type SnapshottingGuages[T any] struct{ Msg T }

// Name returns the name of the event.
func (e SnapshottingGuages[T]) Name() EventName {
	return "snapshotting gauges"
}

// EvtNoSubscribers is emitted when a message is sent but there are no subscribers to receive it.
type EvtNoSubscribers[T any] struct{ Msg T }

// Name returns the name of the event.
func (e EvtNoSubscribers[T]) Name() EventName {
	return "no subscribers"
}

// Message returns the message that was sent but not received by any subscribers.
func (e EvtNoSubscribers[T]) Message() T {
	return e.Msg
}

// EvtSent is emitted when a message is sent to the hub.
type EvtSent[T any] struct{ Msg T }

// Name returns the name of the event.
func (e EvtSent[T]) Name() EventName {
	return "message sent"
}

// Message returns the message that was sent to the hub.
func (e EvtSent[T]) Message() T {
	return e.Msg
}

// EvtDelivered is emitted when a message is delivered to a subscriber.
type EvtDelivered[T any] struct{ Msg T }

// Name returns the name of the event.
func (e EvtDelivered[T]) Name() EventName {
	return "message delivered"
}

// Message returns the message that was delivered to a subscriber.
func (e EvtDelivered[T]) Message() T {
	return e.Msg
}

// EvtSendContextCancelled is emitted when a message send is cancelled due to the context being cancelled.
type EvtSendContextCancelled[T any] struct{ Msg T }

// Name returns the name of the event.
func (e EvtSendContextCancelled[T]) Name() EventName {
	return "send context cancelled"
}

// Message returns the message that was attempted to be sent when the context was cancelled.
func (e EvtSendContextCancelled[T]) Message() T {
	return e.Msg
}

// EvtSubscribed is emitted when a subscription is successfully created.
type EvtSubscribed[T any] struct{ Sub *Subscription[T] }

// Name returns the name of the event.
func (e EvtSubscribed[T]) Name() EventName {
	return "subscribed"
}

// Subscription returns the subscription that was created.
func (e EvtSubscribed[T]) Subscription() *Subscription[T] {
	return e.Sub
}

// EvtCancelled is emitted when a subscription is explicitly cancelled.
type EvtCancelled[T any] struct{ Sub *Subscription[T] }

// Name returns the name of the event.
func (e EvtCancelled[T]) Name() EventName {
	return "cancelled"
}

// Subscription returns the subscription that was cancelled.
func (e EvtCancelled[T]) Subscription() *Subscription[T] {
	return e.Sub
}

// EvtCancelFailed is emitted when a subscription cancellation fails.
type EvtCancelFailed[T any] struct {
	Sub *Subscription[T]
	Err error
}

// Name returns the name of the event.
func (e EvtCancelFailed[T]) Name() EventName {
	return "cancel failed"
}

// Subscription returns the subscription that failed to be cancelled.
func (e EvtCancelFailed[T]) Subscription() *Subscription[T] {
	return e.Sub
}

// EventHandler is a simple interface for handling events.
type EventHandler[T any] interface {
	HandleEvent(context.Context, Event[T])
}

// EventHandlerFunc is a function type that implements the EventHandler interface.
type EventHandlerFunc[T any] func(context.Context, Event[T])

// HandleEvent calls the function with the event.
func (f EventHandlerFunc[T]) HandleEvent(ctx context.Context, e Event[T]) {
	f(ctx, e)
}
