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
	"sync"
	"time"
)

// Subscription represents a subscription to a Hub.
type Subscription[T any] struct {
	ch          chan T
	hub         *Hub[T]
	ctx         context.Context
	timer       *time.Timer
	err         error
	errMu       sync.RWMutex
	maxTimeouts int
}

// newSubscription creates a new subscription to a Hub with the given buffer size and tolerance.
func newSubscription[T any](ctx context.Context, s *Hub[T], opts ...func(*Subscription[T])) *Subscription[T] {
	sub := &Subscription[T]{
		ctx: ctx,
		hub: s,
	}

	for _, opt := range opts {
		opt(sub)
	}

	if sub.ch == nil {
		sub.ch = make(chan T)
	}

	return sub
}

// Data returns a read-only channel that receives data from the subscription.
func (s *Subscription[T]) Data() <-chan T {
	return s.ch
}

// Err returns the error associated with the subscription, if any.
func (s *Subscription[T]) Err() error {
	s.errMu.RLock()
	defer s.errMu.RUnlock()

	return s.err
}

// Cancel cancels the subscription via the associated Hub.
func (s *Subscription[T]) Cancel(ctx context.Context) error {
	return s.hub.Cancel(ctx, s)
}

func (s *Subscription[T]) setErr(err error) {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	s.err = err
}

func (s *Subscription[T]) close() {
	close(s.ch)
}

// WithReceiveBuffer sets the receive buffer size for a subscription.
func WithReceiveBuffer[T any](buffSize int) func(*Subscription[T]) {
	return func(h *Subscription[T]) {
		h.ch = make(chan T, buffSize)
	}
}

// WithMaxDeliveryTimeouts sets the maximum number of timeouts before
// a subscription is dropped.
func WithMaxDeliveryTimeouts[T any](tolerance int) func(*Subscription[T]) {
	return func(h *Subscription[T]) {
		h.maxTimeouts = tolerance
	}
}
