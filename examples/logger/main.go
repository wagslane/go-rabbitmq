package main

import (
	"log"

	rabbitmq "github.com/fortyanov/go-rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// customLogger is used in WithPublisherOptionsLogger to create a custom logger.
type customLogger struct{}

// Printf is the only method needed in the Logger interface to function properly.
func (c *customLogger) Printf(fmt string, args ...interface{}) {
	log.Printf("mylogger: "+fmt, args...)
}

func main() {
	mylogger := &customLogger{}

	publisher, err := rabbitmq.NewPublisher(
		"amqp://guest:guest@localhost", amqp.Config{},
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

	returns := publisher.NotifyReturn()
	go func() {
		for r := range returns {
			log.Printf("message returned from server: %s", string(r.Body))
		}
	}()
}
