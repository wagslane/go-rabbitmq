package main

import (
	"fmt"
	"log"

	rabbitmq "github.com/wagslane/go-rabbitmq"
)

// customLogger is used in WithPublisherOptionsLogger to create a custom logger.
type customLogger struct{}

func (l *customLogger) Debug(s string) {
	log.Println(fmt.Sprintf("[Debug] mylogger: %s", s))
}

func (l *customLogger) Info(s string) {
	log.Println(fmt.Sprintf("[Info] mylogger: %s", s))
}

func (l *customLogger) Warning(s string) {
	log.Println(fmt.Sprintf("[Warning] mylogger: %s", s))
}

func (l *customLogger) Error(s string) {
	log.Println(fmt.Sprintf("[Error] mylogger: %s", s))
}

func main() {
	mylogger := &customLogger{}

	publisher, err := rabbitmq.NewPublisher(
		"amqp://guest:guest@localhost", rabbitmq.Config{},
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
