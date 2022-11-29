# go-rabbitmq

Wrapper of [rabbitmq/amqp091-go](https://github.com/rabbitmq/amqp091-go) that provides reconnection logic and sane defaults. Hit the project with a star if you find it useful ⭐

Supported by [Boot.dev](https://boot.dev)

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

Take note of the optional `options` parameters after the queue name. While not *necessary*, you'll *probably* want to at least declare the queue itself and some routing key bindings.

```go
consumer, err := rabbitmq.NewConsumer(
    "amqp://user:pass@localhost",
    rabbitmq.Config{},
    rabbitmq.WithConsumerOptionsLogging,
)
if err != nil {
    log.Fatal(err)
}
defer consumer.Close()
err = consumer.StartConsuming(
		func(d rabbitmq.Delivery) rabbitmq.Action {
			log.Printf("consumed: %v", string(d.Body))
			// rabbitmq.Ack, rabbitmq.NackDiscard, rabbitmq.NackRequeue
			return rabbitmq.Ack
		},
		"my_queue",
		// spawns 10 goroutines to handle incoming messages
		rabbitmq.WithConsumeOptionsConcurrency(10),
		// assigns a name to this consumer on the cluster
		rabbitmq.WithConsumeOptionsConsumerName(consumerName),
		rabbitmq.WithConsumeDeclareOptions(
			// creates a durable queue named "my_queue"
			// if it doesn't exist yet
			rabbitmq.WithDeclareQueueDurable,
			// binds the queue to "my_routing_key" if it isn't already
			rabbitmq.WithDeclareBindingsForRoutingKeys([]string{"my_routing_key"}),
		),
	)
if err != nil {
    log.Fatal(err)
}
```

## 🚀 Quick Start Publisher

Again, notice that all of the options functions aren't required, use them as you need them.

```go
publisher, err := rabbitmq.NewPublisher(
    "amqp://user:pass@localhost",
    rabbitmq.Config{},
    // can pass nothing for no logging
    rabbitmq.WithPublisherOptionsLogging,
)
defer publisher.Close()
if err != nil {
    log.Fatal(err)
}
err = publisher.Publish(
	[]byte("hello, world"),
	[]string{"routing_key"},
	// optionally set the content type of the published messages
	rabbitmq.WithPublishOptionsContentType("application/json"),
	// optionally auto-declare and bind
	// to this exchange if it doens't exist yet
	rabbitmq.WithPublishOptionsExchange("events"),
)
if err != nil {
    log.Fatal(err)
}

returns := publisher.NotifyReturn()
go func() {
    for r := range returns {
        log.Printf("message returned from server: %s", string(r.Body))
    }
}()
```

## Other usage examples

See the [examples](examples) directory for more ideas.

## Stability

Note that the API is currently in `v0`. I don't plan on any huge changes, but there may be some small breaking changes before we hit `v1`.

## 💬 Contact

[![Twitter Follow](https://img.shields.io/twitter/follow/wagslane.svg?label=Follow%20Wagslane&style=social)](https://twitter.com/intent/follow?screen_name=wagslane)

Submit an issue (above in the issues tab)

## Transient Dependencies

My goal is to keep dependencies limited to 1, [github.com/rabbitmq/amqp091-go](https://github.com/rabbitmq/amqp091-go).

## 👏 Contributing

I would love your help! Contribute by forking the repo and opening pull requests. Please ensure that your code passes the existing tests and linting, and write tests to test your changes if applicable.

All pull requests should be submitted to the `main` branch.
