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

// WithEventHandlerFunc adds an EventHandler for the message stream.
func WithEventHandlerFunc[T any](fn func(context.Context, Event[T])) func(*Hub[T]) {
	return WithEventHandler[T](EventHandlerFunc[T](fn))
}

// WithEventHandler adds an EventHandler for the message stream.
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

// WithEventHandlers sets the EventHandler(s) for the message stream.
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
