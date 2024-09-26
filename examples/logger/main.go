package main

import (
	"context"
	"log"

	rabbitmq "github.com/makometr/go-rabbitmq"
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

func main() {
	mylogger := &errorLogger{}

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
		rabbitmq.WithPublisherOptionsLogger(mylogger),
	)
	if err != nil {
		log.Fatal(err)
	}
	err = publisher.PublishWithContext(
		context.Background(),
		[]byte("hello, world"),
		[]string{"my_routing_key"},
		rabbitmq.WithPublishOptionsContentType("application/json"),
		rabbitmq.WithPublishOptionsMandatory,
		rabbitmq.WithPublishOptionsPersistentDelivery,
		rabbitmq.WithPublishOptionsExchange("events"),
	)
	if err != nil {
		log.Fatal(err)
	}

	publisher.NotifyReturn(func(r rabbitmq.Return) {
		log.Printf("message returned from server: %s", string(r.Body))
	})
}
