package main

import (
	"log"

	rabbitmq "github.com/wagslane/go-rabbitmq"
)

// errorLogger is used in WithPublisherOptionsLogger to create a custom logger
// that only logs ERROR and FATAL log levels
type errorLogger struct{}

func (l errorLogger) Fatalf(format string, v ...interface{}) {
	log.Printf("mylogger: "+format, v...)
}

func (l errorLogger) Errorf(format string, v ...interface{}) {
	log.Printf("mylogger: "+format, v...)
}

func (l errorLogger) Warnf(format string, v ...interface{}) {
}

func (l errorLogger) Infof(format string, v ...interface{}) {
}

func (l errorLogger) Debugf(format string, v ...interface{}) {
}

func (l errorLogger) Tracef(format string, v ...interface{}) {}

func main() {
	mylogger := &errorLogger{}

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
