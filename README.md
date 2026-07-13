# Replicators

Robust value replication library for high concurrency Go applications.

## Use Cases

Any situation where a Go value must be reliably broadcasted to a dynamic list of subscribers.

For example:

 - Republishing data from a message broker to SSE, Websocket or gRPC clients
 - Broadcasting messages between Websocket clients
 - Internal event propagation to client goroutines

### Documentation and Examples

GoDoc including examples are found on [pkg.go.dev](https://pkg.go.dev/github.com/johnknl/replicators).
A synthetic usage example is found in `./examples/sse/main.go`, runnable using `make run-example`.

Although there are other use cases, I created this library for the purpose of scalable edge 
replication of broker messages. The below chart illustrates an example topology.

```mermaid
flowchart LR
    broker["Message Broker<br>Single Topic"]
    consumer["Consumer Goroutine"]
    hub["Hub"]

    broker -->|Consume| consumer
    consumer -->|Broadcast| hub

    hub --> sub1["Subscription"]
    hub --> sub2["Subscription"]
    hub --> sub3["Subscription"]
    hub --> sub4["Subscription"]

    sub1 --> grpc1["gRPC Client"]
    sub2 --> grpc2["gRPC Client"]
    sub3 --> ws1["WebSocket Client"]
    sub4 --> sse1["SSE Client"]
```

## Key Properties

- Online attaching and detaching of subscribers
- Automatic detaching of slow consumers
- Comprehensive event handling mechanism
- No 3rd party dependencies
- Type safe
- Tested, benchmarked, (partly) optimized

## Niceties

- Bundled slog event handler
- Native stat (counters, gauges) handler, useful for integration with eg Prometheus scraping

## Example

Below is the main example used in Godoc:

```go
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
	// &replicators.Counts{Subscriptions:1, Cancellations:1, Sent:4, Dropped:2, Delivered:1, Undeliverable:1}
}
```

## License

MIT
