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
	"log/slog"
	"os"
)

// WithSlogger is a convenience function that sets up a slog.Logger as an event handler for the hub.
func WithSlogger[T any](ctx context.Context, logger *slog.Logger) func(*Options[T]) {
	return WithEventHandler(SlogEventHandler[T](ctx, logger))
}

// WithDevLogger is a convenience function that sets up a development logger using
// the standard library's slog package.
func WithDevLogger[T any]() func(*Options[T]) {
	return WithEventHandler(DevLoggerHandler[T]())
}

// DevLoggerHandler returns an EventHandler that logs events using the standard library's slog package.
func DevLoggerHandler[T any]() EventHandler[T] {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	return SlogEventHandler[T](context.Background(), logger)
}

// SlogEventHandler returns an EventHandler that logs events using the provided slog.Logger.
//
// Note that the supported levels are determined at creation time and will not be reflected
// if the logger's level is changed later. The logger will use the context created with the
// EventHandler, not the context passed when the event is handled.
//
// Disabling warning logging will result in nothing being logged.
func SlogEventHandler[T any](ctx context.Context, logger *slog.Logger) EventHandler[T] {
	if !logger.Enabled(ctx, slog.LevelWarn) {
		return EventHandlerFunc[T](func(context.Context, Event[T]) {})
	}

	debug := logger.Enabled(ctx, slog.LevelDebug)
	info := logger.Enabled(ctx, slog.LevelInfo)

	return EventHandlerFunc[T](func(_ context.Context, e Event[T]) {
		level := slog.LevelInfo
		switch e.(type) {
		case EvtSubDropped[T],
			EvtNoSubscribers[T],
			EvtCancelFailed[T],
			EvtDeliveryTimeout[T],
			EvtSendContextCancelled[T]:
			level = slog.LevelWarn
		case EvtSent[T], EvtDelivered[T]:
			if !debug {
				return
			}
			level = slog.LevelDebug
		default:
			if !info {
				return
			}
		}

		attrs := make([]slog.Attr, 0, 2)

		if evt, ok := e.(interface{ Message() T }); ok && debug {
			attrs = append(attrs, slog.Any("msg", evt.Message()))
		}

		if evt, ok := e.(interface{ Subscription() *Subscription[T] }); ok {
			attrs = append(attrs,
				slog.String("sub", fmt.Sprintf("%p", evt.Subscription())),
			)
		}

		switch evt := e.(type) {
		case EvtDraining:
			attrs = append(attrs, slog.Int("count", evt.Count))
		case EvtDeliveryTimeout[T]:
			attrs = append(attrs, slog.Duration("timeout", evt.Timeout))
		case EvtCancelFailed[T]:
			attrs = append(attrs, slog.String("err", evt.Err.Error()))
		}

		logger.LogAttrs(ctx, level, string(e.Name()), attrs...)
	})
}
