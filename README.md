# go-rabbitmq

Wrapper of streadway/amqp that provides reconnection logic and sane defaults. Hit the project with a star if you find it useful ⭐

Supported by [Qvault](https://qvault.io)

## Motivation

[Streadway's AMQP](https://github.com/streadway/amqp) library is currently the most robust and well-supported Go client I'm aware of. It's a fantastic option and I recommend starting there and seeing if it fulfills your needs. Their project has made an effort to stay within the scope of the AMQP protocol, as such, no reconnection logic and few ease-of-use abstractions are provided.

The goal with `go-rabbitmq` is to still provide most all of the nitty-gritty functionality of AMQP, but to make it easier to work with via a higher-level API. Particularly:

* Automatic reconnection
* Multithreaded consumers via a handler function
* Reasonable defaults
* Flow control handling

## ⚙️ Installation

Outside of a Go module:

```bash
go get github.com/wagslane/go-rabbitmq
```

## 🚀 Quick Start Consumer

```go
consumer, err := rabbitmq.GetConsumer("amqp://user:pass@localhost", true)
if err != nil {
    log.Fatal(err)
}
err = consumer.StartConsumers(
    func(d rabbitmq.Delivery) bool {
        log.Printf("consumed: %v", string(d.Body))

        // true to ACK, false to NACK
        return true
    },
    // can pass nil here for defaults
    &rabbitmq.ConsumeOptions{
        QueueOptions: rabbitmq.QueueOptions{
            Durable: true,
        },
        QosOptions: rabbitmq.QosOptions{
            Concurrency: 10,
            Prefetch:    100,
        },
    },
    "my_queue",
    "routing_key1", "routing_key2",
)
if err != nil {
    log.Fatal(err)
}
```

## 🚀 Quick Start Publisher

```go
publisher, returns, err := rabbitmq.GetPublisher("amqp://user:pass@localhost", true)
if err != nil {
    log.Fatal(err)
}
err = publisher.Publish(
    []byte("hello, world"),
    // leave nil for defaults
    &rabbitmq.PublishOptions{
        Exchange:  "events",
        Mandatory: true,
    },
    "routing_key",
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

## 💬 Contact

[![Twitter Follow](https://img.shields.io/twitter/follow/wagslane.svg?label=Follow%20Wagslane&style=social)](https://twitter.com/intent/follow?screen_name=wagslane)

Submit an issue (above in the issues tab)

## Transient Dependencies

* [github.com/streadway/amqp](https://github.com/streadway/amqp) - and that's it.

## 👏 Contributing

I love help! Contribute by forking the repo and opening pull requests. Please ensure that your code passes the existing tests and linting, and write tests to test your changes if applicable.

All pull requests should be submitted to the `main` branch.
