# go-rabbitmq

A wrapper of [rabbitmq/amqp091-go](https://github.com/rabbitmq/amqp091-go) that provides reconnection logic and sane defaults. Hit the project with a star if you find it useful ⭐

Supported by [Boot.dev](https://boot.dev). If you'd like to learn about RabbitMQ and Go, you can check out [my course here](https://www.boot.dev/courses/learn-pub-sub-rabbitmq-golang).

[![](https://godoc.org/github.com/wagslane/go-rabbitmq?status.svg)](https://godoc.org/github.com/wagslane/go-rabbitmq)![Deploy](https://github.com/wagslane/go-rabbitmq/workflows/Tests/badge.svg)

## Motivation

[Streadway's AMQP](https://github.com/rabbitmq/amqp091-go) library is currently the most robust and well-supported Go client I'm aware of. It's a fantastic option and I recommend starting there and seeing if it fulfills your needs. Their project has made an effort to stay within the scope of the AMQP protocol, as such, no reconnection logic and few ease-of-use abstractions are provided.

### Goal

The goal with `go-rabbitmq` is to provide *most* (but not all) of the nitty-gritty functionality of Streadway's AMQP, but to make it easier to work with via a higher-level API. `go-rabbitmq` is also built specifically for Rabbit, not for the AMQP protocol. In particular, we want:

* Automatic reconnection
* Multithreaded consumers via a handler function
* Reasonable defaults
* Flow control handling
* TCP block handling

## ⚙️ Installation

Inside a Go module:

```bash
go get github.com/wagslane/go-rabbitmq
```

## 🚀 Quick Start Consumer

Take note of the optional `options` parameters after the queue name. The *queue* will be declared automatically, but the *exchange* will not. You'll also *probably* want to bind to at least one routing key.

```go
conn, err := rabbitmq.NewConn(
	"amqp://guest:guest@localhost",
	rabbitmq.WithConnectionOptionsLogging,
)
if err != nil {
	log.Fatal(err)
}
defer conn.Close()

consumer, err := rabbitmq.NewConsumer(
	conn,
	"my_queue",
	rabbitmq.WithConsumerOptionsRoutingKey("my_routing_key"),
	rabbitmq.WithConsumerOptionsExchangeName("events"),
	rabbitmq.WithConsumerOptionsExchangeDeclare,
)
if err != nil {
	log.Fatal(err)
}
defer consumer.Close()

err = consumer.Run(func(d rabbitmq.Delivery) rabbitmq.Action {
	log.Printf("consumed: %v", string(d.Body))
	// rabbitmq.Ack, rabbitmq.NackDiscard, rabbitmq.NackRequeue
	return rabbitmq.Ack
})
if err != nil {
	log.Fatal(err)
}
```

## Consumer startup and buffered work

`Run` blocks for the consumer's lifetime. To wait for initial subscription setup
without waiting for the first message, run it in a goroutine and call `WaitReady`:

```go
runErr := make(chan error, 1)
go func() { runErr <- consumer.Run(handler) }()

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
if err := consumer.WaitReady(ctx); err != nil {
    // The caller decides whether to stop or keep waiting after a timeout.
    consumer.Close()
    return err
}
// This subscription is established, even if the queue is empty.
// Keep runErr for observing Run's eventual return.
```

`WaitReady` returns the initial setup error (QoS, declarations, bindings or
subscription) or success after `basic.consume-ok`. It can be called before `Run`
and by multiple waiters. Canceling its context only cancels that wait. The result
is retained across reconnects; it is not an ongoing health check.
`WithConsumerOptionsConsumerNoWait` skips the broker acknowledgement, so
`WaitReady` returns an error with that option.

Handlers that return `Manual` after buffering deliveries can use
`CloseWithDrain` to finish that work before the AMQP channel closes:

```go
consumer.CloseWithDrain(func() {
    close(batchInput) // No more handlers can send to this local buffer.
    batchWorkers.Wait() // Workers flush and Ack/Nack while the channel is open.
})
```

`CloseWithDrain` always waits for active handlers, including when force shutdown
is configured. The callback runs once, synchronously, as part of shutdown.
Deliveries not admitted to handlers remain unacked until channel close requeues
them. A finite prefetch bounds those deliveries. Network failures may still
interrupt acknowledgements during drain.

## 🚀 Quick Start Publisher

The exchange is not declared by default, that's why I recommend using the following options.
```go
conn, err := rabbitmq.NewConn(
	"amqp://guest:guest@localhost",
	rabbitmq.WithConnectionOptionsLogging,
)
if err != nil {
	log.Fatal(err)
}
defer conn.Close()

publisher, err := rabbitmq.NewPublisher(
	conn,
	rabbitmq.WithPublisherOptionsLogging,
	rabbitmq.WithPublisherOptionsExchangeName("events"),
	rabbitmq.WithPublisherOptionsExchangeDeclare,
)
if err != nil {
	log.Fatal(err)
}
defer publisher.Close()

err = publisher.Publish(
	[]byte("hello, world"),
	[]string{"my_routing_key"},
	rabbitmq.WithPublishOptionsContentType("application/json"),
	rabbitmq.WithPublishOptionsExchange("events"),
)
if err != nil {
	log.Println(err)
}
```

## Other usage examples

See the [examples](examples) directory for more ideas.

## Options and configuring

* By default, queues are declared if they didn't already exist by new consumers
* By default, routing-key bindings are declared by consumers if you're using `WithConsumerOptionsRoutingKey`
* By default, exchanges are *not* declared by publishers or consumers if they don't already exist, hence `WithPublisherOptionsExchangeDeclare` and `WithConsumerOptionsExchangeDeclare`.

Read up on all the options in the GoDoc, there are quite a few of them. I try to pick sane and simple defaults.

## Closing and resources

Close your publishers and consumers when you're done with them and do *not* attempt to reuse them. Only close the connection itself once you've closed all associated publishers and consumers.

## Stability

Note that the API is currently in `v0`. I don't plan on huge changes, but there may be some small breaking changes before we hit `v1`.

## Integration testing

By setting `ENABLE_DOCKER_INTEGRATION_TESTS=TRUE` during `go test -v ./...`, the integration tests will run. These launch a rabbitmq container in the local Docker daemon and test some publish/consume actions.

See [integration_test.go](integration_test.go).

## 💬 Contact

[![Twitter Follow](https://img.shields.io/twitter/follow/wagslane.svg?label=Follow%20Wagslane&style=social)](https://twitter.com/intent/follow?screen_name=wagslane)

Submit an issue here on GitHub

## Transient Dependencies

My goal is to keep dependencies limited to 1, [github.com/rabbitmq/amqp091-go](https://github.com/rabbitmq/amqp091-go).

## 👏 Contributing

I would love your help! Contribute by forking the repo and opening pull requests. Please ensure that your code passes the existing tests and linting, and write tests to test your changes if applicable.

All pull requests should be submitted to the `main` branch.
