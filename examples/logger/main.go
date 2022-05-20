package main

import (
	"log"

	rabbitmq "github.com/wagslane/go-rabbitmq"
)

// errorLogger is used in WithPublisherOptionsLogger to create a custom logger
// that only logs ERROR and FATAL log levels
type errorLogger struct{}

func (l errorLogger) FatalF(format string, v ...interface{}) {
	log.Printf("mylogger: "+format, v...)
}

func (l errorLogger) ErrorF(format string, v ...interface{}) {
	log.Printf("mylogger: "+format, v...)
}

func (l errorLogger) WarnF(format string, v ...interface{}) {
}

func (l errorLogger) InfoF(format string, v ...interface{}) {
}

func (l errorLogger) DebugF(format string, v ...interface{}) {
}

func (l errorLogger) TraceF(format string, v ...interface{}) {}

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
