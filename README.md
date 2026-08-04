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

## Reliable publishing with per-message outcomes

To know the fate of each published message — confirmed, nacked, or returned as
unroutable — use `PublishWithOutcome` on a publisher created with
`WithPublisherOptionsConfirm`. Unlike consuming `NotifyPublish` and
`NotifyReturn` as separate streams, it pairs each message's confirmation with
its return reliably, even with many messages in flight, so you can republish
exactly the failed subset of a batch:

```go
outcomes, err := publisher.PublishWithOutcome(
	ctx,
	[]byte("hello, world"),
	[]string{"my_routing_key"},
	rabbitmq.WithPublishOptionsExchange("events"),
	rabbitmq.WithPublishOptionsMandatory, // required to detect unroutable messages
)
if err != nil {
	log.Println(err)
}
for _, po := range outcomes {
	outcome, err := po.Wait(ctx)
	if err != nil {
		log.Println(err)
	} else if outcome.Failed() {
		// nacked, returned as unroutable, or unknown (channel lost mid-flight):
		// republish, but see ErrOutcomeUnknown about possible duplicates
	}
}
```

Use `WithPublisherOptionsMaxOutcomesInFlight` to bound memory by limiting how
many outcomes may be unresolved at once.

### Correlating outcomes with your data

An `Outcome` reports `ID`, `Exchange`, `RoutingKey` and `DeliveryTag`, but not
the payload — only a message the broker returned as unroutable carries its body
back, on `Return.Body`. To act on a failure you usually need your own record of
it, so attach one with `WithPublishOptionsOutcomeRef` and read it back from
`Outcome.Ref`:

```go
outcomes, err := publisher.PublishWithOutcome(
	ctx,
	row.Body,
	[]string{row.RoutingKey},
	rabbitmq.WithPublishOptionsMandatory,
	rabbitmq.WithPublishOptionsOutcomeRef(row), // your value, echoed back
)
if err != nil {
	log.Println(err)
}
for _, po := range outcomes {
	outcome, err := po.Wait(ctx)
	if err != nil {
		log.Println(err)
	}
	if outcome.Failed() {
		row := outcome.Ref.(*Row) // republish it, or mark it in your database
		log.Printf("row %d failed: %v", row.ID, outcome.Err)
	}
}
```

The ref is opaque to the library and is never sent to the broker — unlike
`CorrelationID`, which is an AMQP property your consumers can see. It is held
only until the outcome resolves, so `WithPublisherOptionsMaxOutcomesInFlight`
bounds how much it can retain. Every outcome of one call carries the same ref;
use `RoutingKey` to tell them apart, and `PublishOutcome.Ref` to read it before
the outcome resolves.

A few other things worth knowing:

- `Outcome.ID` is the value the library puts on the wire as the
  `x-gorabbitmq-outcome-id` header, so you can match a publisher outcome against
  the delivery your consumer received. Prefer it over `DeliveryTag`, which is
  per-channel and restarts at 1 after a reconnect.
- If you would rather keep the correlation statically typed, `*PublishOutcome` is
  a unique handle and works as a map key: `map[*PublishOutcome]*Row`.
- When `Wait` returns a context error the message is still in flight. The
  returned `Outcome` carries the identity and ref anyway, with `Err` set to the
  context error, so you can record which message you stopped waiting on.

See [examples/publisher_outcome](examples/publisher_outcome) for a complete batch
publish-and-retry example.

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
