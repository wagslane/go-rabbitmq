# go-rabbitmq

Wrapper of [rabbitmq/amqp091-go](https://github.com/rabbitmq/amqp091-go) that provides reconnection logic and sane defaults. Hit the project with a star if you find it useful ⭐

Supported by [Qvault](https://qvault.io)

[![](https://godoc.org/github.com/wagslane/go-rabbitmq?status.svg)](https://godoc.org/github.com/wagslane/go-rabbitmq)![Deploy](https://github.com/wagslane/go-rabbitmq/workflows/Tests/badge.svg)

## Motivation

[Streadway's AMQP](https://github.com/rabbitmq/amqp091-go) library is currently the most robust and well-supported Go client I'm aware of. It's a fantastic option and I recommend starting there and seeing if it fulfills your needs. Their project has made an effort to stay within the scope of the AMQP protocol, as such, no reconnection logic and few ease-of-use abstractions are provided.

### Goal 

The goal with `go-rabbitmq` is to still provide most all of the nitty-gritty functionality of AMQP, but to make it easier to work with via a higher-level API. Particularly:

* Automatic reconnection
* Multithreaded consumers via a handler function
* Reasonable defaults
* Flow control handling

## ⚙️ Installation

Inside a Go module:

```bash
go get github.com/wagslane/go-rabbitmq
```

## 🚀 Quick Start Consumer

### Default options

```go
consumer, err := rabbitmq.NewConsumer("amqp://user:pass@localhost", amqp.Config{})
if err != nil {
    log.Fatal(err)
}
err = consumer.StartConsuming(
    func(d rabbitmq.Delivery) rabbitmq.Action {
        log.Printf("consumed: %v", string(d.Body))
        // rabbitmq.Ack, rabbitmq.NackDiscard, rabbitmq.NackRequeue
        return rabbitmq.Ack
    },
    "my_queue",
    []string{"routing_key1", "routing_key2"}
)
if err != nil {
    log.Fatal(err)
}
```

### With options

```go
consumer, err := rabbitmq.NewConsumer(
    "amqp://user:pass@localhost",
    amqp091.Config{},
    rabbitmq.WithConsumerOptionsLogging,
)
if err != nil {
    log.Fatal(err)
}
err = consumer.StartConsuming(
		func(d rabbitmq.Delivery) rabbitmq.Action {
			log.Printf("consumed: %v", string(d.Body))
			// rabbitmq.Ack, rabbitmq.NackDiscard, rabbitmq.NackRequeue
			return rabbitmq.Ack
		},
		"my_queue",
		[]string{"routing_key", "routing_key_2"},
		rabbitmq.WithConsumeOptionsConcurrency(10),
		rabbitmq.WithConsumeOptionsQueueDurable,
		rabbitmq.WithConsumeOptionsQuorum,
		rabbitmq.WithConsumeOptionsBindingExchangeName("events"),
		rabbitmq.WithConsumeOptionsBindingExchangeKind("topic"),
		rabbitmq.WithConsumeOptionsBindingExchangeDurable,
		rabbitmq.WithConsumeOptionsConsumerName(consumerName),
	)
if err != nil {
    log.Fatal(err)
}
```

## 🚀 Quick Start Publisher

### Default options

```go
publisher, returns, err := rabbitmq.NewPublisher("amqp://user:pass@localhost", amqp091.Config{})
if err != nil {
    log.Fatal(err)
}
err = publisher.Publish([]byte("hello, world"), []string{"routing_key"})
if err != nil {
    log.Fatal(err)
}
```

### With options

```go
publisher, returns, err := rabbitmq.NewPublisher(
    "amqp://user:pass@localhost",
    amqp091.Config{},
    // can pass nothing for no logging
    rabbitmq.WithPublisherOptionsLogging,
)
if err != nil {
    log.Fatal(err)
}
err = publisher.Publish(
	[]byte("hello, world"),
	[]string{"routing_key"},
	rabbitmq.WithPublishOptionsContentType("application/json"),
	rabbitmq.WithPublishOptionsMandatory,
	rabbitmq.WithPublishOptionsPersistentDelivery,
	rabbitmq.WithPublishOptionsExchange("events"),
)
if err != nil {
    log.Fatal(err)
}

go func() {
    for r := range returns {
        log.Printf("message returned from server: %s", string(r.Body))
    }
}()
```

## Other usage examples

See the [examples](examples) directory for more ideas.

## 💬 Contact

[![Twitter Follow](https://img.shields.io/twitter/follow/wagslane.svg?label=Follow%20Wagslane&style=social)](https://twitter.com/intent/follow?screen_name=wagslane)

Submit an issue (above in the issues tab)

## Transient Dependencies

My goal is to keep dependencies limited to 1, [github.com/rabbitmq/amqp091-go](https://github.com/rabbitmq/amqp091-go).

## 👏 Contributing

I love help! Contribute by forking the repo and opening pull requests. Please ensure that your code passes the existing tests and linting, and write tests to test your changes if applicable.

All pull requests should be submitted to the `main` branch.
