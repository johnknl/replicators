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

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/johnknl/replicators"
)

type t []byte

var (
	logger      *slog.Logger
	producers   = map[string]*producer{}
	producersMu sync.Mutex
)

func init() {
	logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

type producer struct {
	pause   chan struct{}
	hub     *replicators.Hub[t]
	name    string
	running atomic.Bool
}

func (p *producer) Start(ctx context.Context) {
	if !p.running.CompareAndSwap(false, true) {
		return
	}

	go func() {
		timer := time.NewTicker(1 * time.Second)
		defer timer.Stop()
		defer func() {
			p.running.Store(false)
		}()

		for {
			select {
			case <-p.pause:
				logger.Info("stopping message production")
				return
			case <-ctx.Done():
				logger.Info("cancelled message production", "err", ctx.Err())
				return
			case <-timer.C:
				ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond) // nolint:govet // intentional shadow
				msg := fmt.Appendf(nil, "Welcome to %s! %s", p.name, time.Now().Format(time.RFC3339))
				if err := p.hub.Broadcast(ctx, msg); err != nil {
					logger.Info("failed to broadcast", "err", err)
				}
				cancel()
			}
		}
	}()
}

func (p *producer) Pause() error {
	select {
	case p.pause <- struct{}{}:
		return nil
	default:
		return errors.New("producer not running")
	}
}

func newProducer(ctx context.Context, name string) *producer {
	p := &producer{
		name:  name,
		pause: make(chan struct{}, 1),
	}

	hub := replicators.NewHub[t](
		ctx,
		replicators.WithEchoBuffer[t](3),
		replicators.WithEventHandlers[t](
			replicators.SlogEventHandler[t](ctx, logger),
			replicators.EventHandlerFunc[t](func(_ context.Context, evt replicators.Event[t]) {
				switch evt.(type) {
				case replicators.EvtNoSubscribers[t]:
					if err := p.Pause(); err != nil {
						logger.Error("failed to stop producer", "err", err)
					}
				case replicators.EvtSubscribed[t]:
					p.Start(ctx)
				}
			}),
		),
	)

	p.hub = hub

	return p
}

func getProducer(ctx context.Context, name string) *producer {
	producersMu.Lock()
	defer producersMu.Unlock()

	p, ok := producers[name]
	if !ok {
		p = newProducer(ctx, name)
		producers[name] = p
	}

	return p
}

func getHub(ctx context.Context, name string) *replicators.Hub[t] {
	p := getProducer(ctx, name)

	return p.hub
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp4", ":9001")
	if err != nil {
		logger.Error("failed to listen", "err", err)
		os.Exit(1)
	}

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	var unauthorized = ws.RejectConnectionError(
		ws.RejectionReason("unauthorized"),
		ws.RejectionStatus(http.StatusUnauthorized),
	)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() == nil {
				logger.Error("failed to accept connection", "err", err)
			}
			break
		}

		go func(conn net.Conn) {
			var uri string
			var authorized bool

			u := ws.Upgrader{
				OnHeader: func(key, value []byte) error {
					if string(key) != "Authorization" {
						return nil
					}

					if string(value) != "Bearer secret" {
						return unauthorized
					}

					authorized = true

					return nil
				},
				OnRequest: func(u []byte) error {
					uri = string(u)
					return nil
				},
				OnBeforeUpgrade: func() (ws.HandshakeHeader, error) {
					if !authorized {
						return nil, unauthorized
					}

					return nil, nil
				},
			}

			_, err = u.Upgrade(conn)
			if err != nil {
				logger.Error("failed to upgrade connection", "err", err)
				_ = conn.Close()
				return
			}

			defer conn.Close() // nolint:errcheck // just an example
			hub := getHub(ctx, uri)

			consumerCtx, cancel := context.WithCancel(ctx)

			// read pump (required for protocol handling, but we don't care about the messages)
			go func() {
				defer cancel()

				for {
					_, _, err := wsutil.ReadClientData(conn)
					if err != nil {
						return
					}
				}
			}()

			sub, err := hub.Subscribe(consumerCtx)
			if err != nil {
				logger.Error("failed to subscribe", "err", err)
				return
			}

			for msg := range sub.Data() {
				logger.Debug("writing message to client", "msg", string(msg))
				err := wsutil.WriteServerMessage(conn, ws.OpText, msg)
				if err != nil {
					if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
						logger.Info("client went away")
					} else {
						logger.Error("failed to write message", "err", err)
					}

					// this will close the channel
					if err = sub.Cancel(ctx); !errors.Is(err, replicators.ErrSubscriptionCancelled) {
						logger.Error("failed to cancel subscription", "err", err)
					}

					// but it might not be blocking depending on current defaults
					return
				}
			}
		}(conn)
	}
}
