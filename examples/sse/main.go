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
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/johnknl/replicators"
)

type t []byte

var logger *slog.Logger

func init() {
	logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func produceMessages(ctx context.Context, hub *replicators.Hub[t]) {
	timer := time.NewTicker(500 * time.Millisecond)
	for range timer.C {
		ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond) // nolint:govet // intentional shadow
		if err := hub.Broadcast(ctx, fmt.Appendf(nil, "Hello, World! %d", time.Now().UnixNano())); err != nil {
			logger.Info("failed to broadcast", "err", err)
		}
		cancel()
	}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	hub := replicators.NewHub[t](
		ctx,
		replicators.WithEventHandlers[t](
			replicators.SlogEventHandler[t](ctx, logger),
			replicators.EventHandlerFunc[t](func(_ context.Context, evt replicators.Event[t]) {
				if _, ok := evt.(replicators.EvtNoSubscribers[t]); ok {
					cancel()
				}
			}),
		),
		replicators.WithSendBuffer[t](0),
		replicators.WithAttachBuffer[t](0),
		replicators.WithCancelBuffer[t](0),
	)

	server := &http.Server{
		Addr:              ":8888",
		ReadHeaderTimeout: 5 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sub, err := hub.Subscribe(r.Context(), 10, 0)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("Connection", "keep-alive")

		Loop:
			for { // nolint:staticcheck // range not possible because of open check
				select {
				case msg, ok := <-sub.Data():
					if !ok {
						if err := sub.Err(); err != nil && !errors.Is(err, context.Canceled) {
							http.Error(w, err.Error(), http.StatusInternalServerError)
						}
						break Loop
					}
					_, _ = fmt.Fprintf(w, "data: %s\n\n", string(msg))

					w.(http.Flusher).Flush() // nolint:errcheck // odd

				}
			}

			logger.Info("clean")
		}),
	}

	go func() {
		<-ctx.Done()
		logger.Info("shutting down...")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil { // nolint:contextcheck // intentional
			logger.Error("failed to shutdown server", "err", err)
		}
	}()

	wg := &sync.WaitGroup{}
	wg.Go(func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
	})

	for i := range 10 {
		wg.Go(func() {
			resp, err := http.Get("http://localhost:8888") // nolint:noctx // example
			if err != nil {
				logger.Error("failed to get", "err", err)
				return
			}
			defer resp.Body.Close() // nolint:errcheck // example
			buf := make([]byte, 1024)
			x := 0
			for {
				if x > i+1*i+1 {
					return
				}
				x++
				_, err := resp.Body.Read(buf)
				if err != nil {
					if !errors.Is(err, io.EOF) {
						logger.Error("read failed", "err", err)
					}
					return
				}
			}
		})
	}

	go produceMessages(ctx, hub)

	wg.Wait()
}
