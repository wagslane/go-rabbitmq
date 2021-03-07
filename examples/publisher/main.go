package main

import (
	"log"

	rabbitmq "github.com/wagslane/go-rabbitmq"
)

func main() {
	publisher, returns, err := rabbitmq.GetPublisher(
		"amqp://user:pass@localhost",
		// can pass nothing for no logging
		func(opts *rabbitmq.PublisherOptions) {
			opts.Logging = true
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	err = publisher.Publish(
		[]byte("hello, world"),
		[]string{"routing_key"},
		// leave blank for defaults
		func(opts *rabbitmq.PublishOptions) {
			opts.DeliveryMode = rabbitmq.Persistent
			opts.Mandatory = true
			opts.ContentType = "application/json"
		},
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
