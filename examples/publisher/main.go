package main

import (
	"log"

	rabbitmq "github.com/wagslane/go-rabbitmq"
)

func main() {
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
}
