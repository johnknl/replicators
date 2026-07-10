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

func newLoop[T any](options *Options[T]) *loop[T] {
	return &loop[T]{
		events:               options.EventHandler,
		counters:             options.counters,
		messages:             make(chan T, options.SendBuffer),
		subscriptions:        make([]*Subscription[T], 0),
		attach:               make(chan *Subscription[T], options.AttachBuffer),
		cancel:               make(chan *Subscription[T], options.CancelBuffer),
		gauges:               make(chan *Gauges),
		sample:               make(chan struct{}),
		deliveryTimeout:      options.DeliveryTimeout,
		drainDeliveryTimeout: options.DrainDeliveryTimeout,
	}
}

type loop[T any] struct {
	counters *counters[T]
	events   EventHandler[T]
	messages chan T
	attach   chan *Subscription[T]
	cancel   chan *Subscription[T]
	gauges   chan *Gauges
	sample   chan struct{}

	subscriptions        []*Subscription[T]
	deliveryTimeout      time.Duration
	drainDeliveryTimeout time.Duration
}

func (s *loop[T]) main(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			shutdownContext, cancel := context.WithTimeout(context.Background(), s.drainDeliveryTimeout)
			s.shutdown(shutdownContext) // nolint:contextcheck // we want to use a new context here
			cancel()
			return
		case <-s.sample:
			bufLen := 0
			for _, ss := range s.subscriptions {
				bufLen += len(ss.ch)
			}
			s.gauges <- &Gauges{
				Queued:        len(s.messages),
				Subscriptions: len(s.subscriptions),
				Waiting:       len(s.attach),
				Cancelling:    len(s.cancel),
				Buffered:      bufLen,
			}
		case msg := <-s.messages:
			s.replicate(ctx, msg)
		case sub := <-s.attach:
			s.subscriptions = append(s.subscriptions, sub)
			if s.events != nil {
				s.events.HandleEvent(ctx, EvtSubscribed[T]{Sub: sub})
			}
		case sub := <-s.cancel:
			n := 0
			for _, ss := range s.subscriptions {
				if ss != sub {
					s.subscriptions[n] = ss
					n++
				} else {
					if s.events != nil {
						s.events.HandleEvent(ctx, EvtCancelled[T]{Sub: sub})
					}
				}
			}
			clear(s.subscriptions[n:])
			s.subscriptions = s.subscriptions[:n]
		}
	}
}

func (s *loop[T]) stats(ctx context.Context) *Stats {
	if s.events != nil {
		s.events.HandleEvent(ctx, SnapshottingGuages[T]{})
	}

	var gauges *Gauges
	select {
	case s.sample <- struct{}{}:
		gauges = <-s.gauges
	case <-ctx.Done():
		// fall through
	}

	var counts *Counts

	if s.counters != nil {
		counts = s.counters.snap()
	}

	return &Stats{
		Gauges: gauges,
		Counts: counts,
	}
}

func (s *loop[T]) replicate(ctx context.Context, msg T) { // nolint: gocyclo // won't fix
	if len(s.subscriptions) == 0 {
		if s.events != nil {
			s.events.HandleEvent(ctx, EvtNoSubscribers[T]{Msg: msg})
		}
		return
	}

	n := 0
	for _, sub := range s.subscriptions {
		healthy := true
		select {
		case <-sub.ctx.Done(): // assert subscription context not previously canceled
			if s.events != nil {
				s.events.HandleEvent(ctx, EvtCancelled[T]{Sub: sub})
			}
			healthy = false
			sub.setErr(sub.ctx.Err())
			sub.close()
		case sub.ch <- msg: // check if the channel is even blocked before using a timer
			if s.events != nil {
				s.events.HandleEvent(ctx, EvtDelivered[T]{Msg: msg})
			}
		default:
			if sub.timer == nil {
				sub.timer = time.NewTimer(s.deliveryTimeout)
			}

			timer := sub.timer
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(s.deliveryTimeout)
			select {
			case sub.ch <- msg: // send the message
				if s.events != nil {
					s.events.HandleEvent(ctx, EvtDelivered[T]{Msg: msg})
				}
			case <-timer.C: // or stream level timeout
				if s.events != nil {
					s.events.HandleEvent(ctx, EvtDeliveryTimeout[T]{Msg: msg, Timeout: s.deliveryTimeout})
				}

				sub.tolerance--

				if sub.tolerance <= 0 {
					healthy = false
					sub.setErr(ErrSubscriptionDropped)
					sub.close()

					if s.events != nil {
						s.events.HandleEvent(ctx, EvtSubDropped[T]{Sub: sub})
					}
				}
			case <-sub.ctx.Done(): // or subscription context canceled
				healthy = false
				sub.setErr(sub.ctx.Err())

				if s.events != nil {
					s.events.HandleEvent(ctx, EvtCancelled[T]{Sub: sub})
				}
			}

			timer.Stop()
		}

		if healthy {
			s.subscriptions[n] = sub
			n++
		}
	}

	clear(s.subscriptions[n:])
	s.subscriptions = s.subscriptions[:n]
}

func (s *loop[T]) shutdown(ctx context.Context) {
	if len(s.messages) > 0 {
		if s.events != nil {
			s.events.HandleEvent(ctx, EvtDraining{Count: len(s.messages)})
		}

		// empty the send buffer
	Drain:
		for len(s.messages) > 0 {
			select {
			case <-ctx.Done():
				if s.events != nil {
					s.events.HandleEvent(ctx, EvtDrainCancelled{Remaining: len(s.messages)})
				}
				break Drain
			case msg := <-s.messages:
				s.replicate(ctx, msg)
			}
		}
	}

	if len(s.subscriptions) > 0 {
		for _, sub := range s.subscriptions {
			sub.close()
		}
	}

	// nil some things for GC
	s.subscriptions = nil
	s.messages = nil
	s.attach = nil
	s.cancel = nil
	s.gauges = nil
	s.sample = nil

	if s.events != nil {
		s.events.HandleEvent(ctx, EvtShutdown{})
	}

	s.events = nil
}
