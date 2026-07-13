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
	"sync"
	"testing"
)

// BenchmarkReplication benchmarks the e2e replication of messages to multiple consumers.
func BenchmarkReplication(b *testing.B) {
	n := 10_000
	yConsumers := []int{10, 1000}

	for _, y := range yConsumers {
		b.Run(fmt.Sprintf("%d consumers", y), func(b *testing.B) {
			for b.Loop() {
				ctx, cancel := context.WithCancel(b.Context())
				wg := &sync.WaitGroup{}

				// For the test we'll disable the attach buffer to avoid
				// extra synchronization while ensuring that the broadcast
				// only starts after all consumers have subscribed.
				hub := NewHub(ctx, WithAttachBuffer[int](0))

				for range y {
					subscription, err := hub.Subscribe(ctx, WithReceiveBuffer[int](10))
					if err != nil {
						b.Fatalf("failed to subscribe: %v", err)
					}

					wg.Go(func() {
						for range n {
							<-subscription.Data()
						}
					})
				}

				for i := range n {
					if err := hub.Broadcast(ctx, i); err != nil {
						b.Fatalf("failed to broadcast: %v", err)
					}
				}

				wg.Wait()
				cancel()
			}
		})
	}
}
