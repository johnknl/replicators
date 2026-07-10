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

// Options is a struct that holds the configuration options for the message stream.
type Options[T any] struct {
	counters *counters[T]

	EventHandler         EventHandler[T]
	SendBuffer           int
	AttachBuffer         int
	CancelBuffer         int
	DeliveryTimeout      time.Duration
	DrainDeliveryTimeout time.Duration
}

// NewOptions creates a new Options struct with the given modifiers applied.
// The defaults are somewhat liberal with buffering, which may not suit
// your use case if you prefer durability over throughput.
func NewOptions[T any](modifiers ...func(*Options[T])) *Options[T] {
	options := &Options[T]{
		SendBuffer:           10,
		AttachBuffer:         1,
		CancelBuffer:         10,
		DeliveryTimeout:      500 * time.Millisecond,
		DrainDeliveryTimeout: 500 * time.Millisecond,
	}

	for _, modifier := range modifiers {
		modifier(options)
	}

	return options
}

// WithEventHandlerFunc adds an EventHandler for the message stream.
func WithEventHandlerFunc[T any](fn func(context.Context, Event[T])) func(*Options[T]) {
	return WithEventHandler[T](EventHandlerFunc[T](fn))
}

// WithEventHandler adds an EventHandler for the message stream.
// It will wrap any existing EventHandler.To set all event handlers,
// use WithEventHandlers instead.
func WithEventHandler[T any](handler EventHandler[T]) func(*Options[T]) {
	return func(s *Options[T]) {
		if s.EventHandler == nil {
			s.EventHandler = handler
			return
		}

		oldHandler := s.EventHandler

		s.EventHandler = EventHandlerFunc[T](func(ctx context.Context, e Event[T]) {
			oldHandler.HandleEvent(ctx, e)
			handler.HandleEvent(ctx, e)
		})
	}
}

// WithEventHandlers sets the EventHandler(s) for the message stream.
func WithEventHandlers[T any](handlers ...EventHandler[T]) func(*Options[T]) {
	if len(handlers) > 1 {
		return func(s *Options[T]) {
			s.EventHandler = EventHandlerFunc[T](func(ctx context.Context, e Event[T]) {
				for _, h := range handlers {
					h.HandleEvent(ctx, e)
				}
			})
		}
	}

	return func(s *Options[T]) {
		s.EventHandler = handlers[0]
	}
}

// WithSendBuffer sets the buffer size for the send channel.
func WithSendBuffer[T any](size int) func(*Options[T]) {
	return func(s *Options[T]) {
		s.SendBuffer = size
	}
}

// WithAttachBuffer sets the buffer size for the attach channel.
// The attach channel is used to queue new subscribers.
// Note that on shutdown, the attach buffer is discarded.
func WithAttachBuffer[T any](size int) func(*Options[T]) {
	return func(s *Options[T]) {
		s.AttachBuffer = size
	}
}

// WithCancelBuffer sets the buffer size for the cancel channel.
// The cancel channel is used to cancel subscriptions.
// Note that on shutdown, the cancel buffer is discarded.
func WithCancelBuffer[T any](size int) func(*Options[T]) {
	return func(s *Options[T]) {
		s.CancelBuffer = size
	}
}

// WithDeliveryTimeout sets the maximum time to wait for a subscriber to process a message.
func WithDeliveryTimeout[T any](timeout time.Duration) func(*Options[T]) {
	return func(s *Options[T]) {
		s.DeliveryTimeout = timeout
	}
}

// WithShutdownTimeout sets the maximum time for each subscriber to finish processing
// messages during shutdown. Conceptually this is very similar to delivery timeout,
func WithShutdownTimeout[T any](timeout time.Duration) func(*Options[T]) {
	return func(s *Options[T]) {
		s.DrainDeliveryTimeout = timeout
	}
}
