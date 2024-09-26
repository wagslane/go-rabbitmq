# go-rabbitmq

A wrapper of [rabbitmq/amqp091-go](https://github.com/rabbitmq/amqp091-go) that provides reconnection logic and sane defaults. Hit the project with a star if you find it useful ⭐

Supported by [Boot.dev](https://boot.dev). If you'd like to learn about RabbitMQ and Go, you can check out [my course here](https://www.boot.dev/learn/learn-pub-sub).

[![](https://godoc.org/github.com/makometr/go-rabbitmq?status.svg)](https://godoc.org/github.com/makometr/go-rabbitmq)![Deploy](https://github.com/makometr/go-rabbitmq/workflows/Tests/badge.svg)

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
go get github.com/makometr/go-rabbitmq
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
