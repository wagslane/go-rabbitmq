package main

import (
	"log"

	rabbitmq "github.com/wagslane/go-rabbitmq"
)

// CustomLog is used in WithPublisherOptionsLogger to create a custom logger.
type CustomLog struct{}

func (c *CustomLog) Printf(fmt string, args ...interface{}) {
	log.Printf("mylogger: "+fmt, args...)
}

func main() {
	mylogger := &CustomLog{}

	publisher, returns, err := rabbitmq.NewPublisher(
		"amqp://guest:guest@localhost",
		rabbitmq.WithPublisherOptionsLogger(mylogger),
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
}
