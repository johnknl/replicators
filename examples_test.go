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

	subscription, err := hub.Subscribe(ctx, replicators.WithReceiveBuffer[MyMsg](buffSize))
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

func ExampleWithEchoEnabled() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dropped := make(chan struct{})
	hub := replicators.NewHub(ctx,
		replicators.WithDevLogger[MyMsg](),
		replicators.WithEchoEnabled[MyMsg](),
		replicators.WithEventHandlerFunc[MyMsg](func(_ context.Context, e replicators.Event[MyMsg]) {
			if _, ok := e.(replicators.EvtSubDropped[MyMsg]); ok {
				close(dropped)
			}
		}),
		replicators.WithCounterHandler[MyMsg](),
	)

	// Broadcast a message, it'll be undeliverable because there are no subscribers,
	// but the hub will remember it and send it to the next subscriber.
	// This would be better accomplished with a send buffer but this is just
	// part of the example.
	if err := hub.Broadcast(ctx, MyMsg(42)); err != nil {
		panic(err)
	}

	// Subscribe to the hub, the subscriber will receive the last message
	// that was broadcasted.
	subscription, err := hub.Subscribe(ctx)
	if err != nil {
		panic(err)
	}

	msg := <-subscription.Data()

	fmt.Println(msg) // nolint:forbidigo // example code

	// Now broadcast another message, the subscriber will receive it.
	if err = hub.Broadcast(ctx, MyMsg(43)); err != nil {
		panic(err)
	}

	msg = <-subscription.Data()

	fmt.Println(msg) // nolint:forbidigo // example code

	// Now subscribe to the hub again, the subscriber will receive the last message
	// that was broadcasted.
	subscription2, err := hub.Subscribe(ctx)
	if err != nil {
		panic(err)
	}

	msg = <-subscription2.Data()

	fmt.Println(msg) // nolint:forbidigo // example code

	// Now subscribe again, but don't read the message: subscriber is
	// immediately subject to max delivery timeout (which defaults to 0 timeouts of 100ms).
	_, err = hub.Subscribe(ctx)
	if err != nil {
		panic(err)
	}

	// Wait for the subscription to be dropped by the hub.
	<-dropped

	fmt.Printf("%#v\n", hub.Stats(ctx).Counts) // nolint:forbidigo // example code

	// Output:
	// 42
	// 43
	// 43
	// &replicators.Counts{Subscriptions:3, Cancellations:0, Sent:2, Timeouts:1, Dropped:1, Delivered:3, Undeliverable:1}
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

	start := make(chan struct{})
	subbed := make(chan struct{})
	dropped := make(chan struct{})

	hub := replicators.NewHub(
		ctx,
		replicators.WithDevLogger[MyMsg](),
		replicators.WithCounterHandler[MyMsg](),
		replicators.WithEventHandler(replicators.EventHandlerFunc[MyMsg](func(_ context.Context, e replicators.Event[MyMsg]) {
			switch e.(type) {
			case replicators.EvtSubscribed[MyMsg]:
				close(subbed)
			case replicators.EvtSubDropped[MyMsg]:
				close(dropped)
			}
		})),
	)

	wg := sync.WaitGroup{}

	wg.Go(func() {
		// 1. The first message is never received
		// 2. One message is read
		// 3. Next delivery will be dropped by the hub (but tolerated)
		// 4. Final delivery will be dropped by the hub, and the subscription will be dropped
		for i := range 4 {
			_ = hub.Broadcast(ctx, MyMsg(i))
			if i == 0 {
				close(start)
				<-subbed
			}
		}
	})

	wg.Go(func() {
		// Don't start the next consumer until the first send is dropped
		<-start

		subscription, err := hub.Subscribe(ctx, replicators.WithMaxDeliveryTimeouts[MyMsg](1))
		if err != nil {
			panic(err)
		}

		// We'll read one message, then block until the subscription is
		// dropped by the hub.
		<-subscription.Data()
		<-dropped

		fmt.Println("error: " + subscription.Err().Error()) // nolint:forbidigo // example code
	})

	wg.Wait()

	fmt.Printf("%#v\n", hub.Stats(ctx).Counts) // nolint:forbidigo // example code

	// Output:
	// error: subscription dropped
	// &replicators.Counts{Subscriptions:1, Cancellations:0, Sent:4, Timeouts:2, Dropped:1, Delivered:1, Undeliverable:1}
}
