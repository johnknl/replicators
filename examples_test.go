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

package replicators_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/johnknl/replicators"
)

type MyMsg int

func ExampleHub_Subscribe_withReceiveBuffer() {
	buffSize := 10
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	subbed := make(chan struct{})

	hub := replicators.NewHub(
		ctx,
		replicators.WithDevLogger[MyMsg](),
		replicators.WithEventHandlerFunc[MyMsg](func(_ context.Context, e replicators.Event[MyMsg]) {
			switch e.(type) {
			case replicators.EvtSubscribed[MyMsg]:
				// Because the default settings use a non-zero attach buffer, some of the data would be
				// missed unless we sync. It would be more straightforward to use a zero attach buffer,
				// but this is just an intentionally belabored example.
				close(subbed)
			}
		}),
	)

	subscription, err := hub.Subscribe(ctx, buffSize, 0)
	if err != nil {
		panic(err)
	}

	// Wait for the subscription to be fully attached before broadcasting messages.
	<-subbed

	// fill up the buffer
	for i := range buffSize {
		_ = hub.Broadcast(ctx, MyMsg(i))
	}

	// read the messages from the receive buffer
	for msg := range subscription.Data() {
		fmt.Println(msg) // nolint:forbidigo // example code
	}

	// Output:
	// 0
	// 1
	// 2
	// 3
	// 4
	// 5
	// 6
	// 7
	// 8
	// 9
}

func ExampleWithSendBuffer() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replicators.NewHub(ctx, replicators.WithSendBuffer[MyMsg](3))
}

func ExampleWithAttachBuffer() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replicators.NewHub(
		ctx,
		replicators.WithAttachBuffer[MyMsg](3),
	)
}

func ExampleWithSlogger() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	replicators.NewHub(ctx, replicators.WithSlogger[MyMsg](ctx, logger))
}

func Example() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	dropped := make(chan struct{})

	hub := replicators.NewHub(
		ctx,
		// replicators.WithDevLogger[MyMsg](),
		replicators.WithCounterHandler[MyMsg](),
		replicators.WithEventHandler(replicators.EventHandlerFunc[MyMsg](func(_ context.Context, e replicators.Event[MyMsg]) {
			switch e.(type) {
			case replicators.EvtSubDropped[MyMsg]:
				close(dropped)
			}
		})),
	)

	// Allow one message to be dropped
	// First message is read, second delivery is dropped.
	// After the third delivery is dropped, the subscription will be dropped.
	subscription, err := hub.Subscribe(ctx, 0, 1)
	if err != nil {
		panic(err)
	}

	wg := sync.WaitGroup{}
	wg.Go(func() {
		// We'll read one message, then block until the subscription is
		// dropped by the hub.
		<-subscription.Data()
		<-dropped

		fmt.Println("error: " + subscription.Err().Error()) // nolint:forbidigo // example code
	})

	for i := range 3 {
		_ = hub.Broadcast(ctx, MyMsg(i+1))
	}

	wg.Wait()

	fmt.Printf("%#v\n", hub.Stats(ctx).Counts) // nolint:forbidigo // example code

	// Output:
	// error: subscription dropped
	// &replicators.Counts{Subscriptions:1, Cancellations:1, Sent:3, Dropped:1, Delivered:1, Undeliverable:1}
}
