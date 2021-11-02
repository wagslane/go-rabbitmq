package main

import (
	"log"

	rabbitmq "github.com/claranet/go-rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	consumer, err := rabbitmq.NewConsumer(
		"amqp://guest:guest@localhost", amqp.Config{},
		rabbitmq.WithConsumerOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}
	err = consumer.StartConsuming(
		func(d rabbitmq.Delivery) (action rabbitmq.Action) {
			log.Printf("consumed: %v", string(d.Body))
			return
		},
		"my_queue",
		[]string{"routing_key", "routing_key_2"},
		rabbitmq.WithConsumeOptionsConcurrency(10),
		rabbitmq.WithConsumeOptionsQueueDurable,
		rabbitmq.WithConsumeOptionsQuorum,
		rabbitmq.WithConsumeOptionsBindingExchangeName("events"),
		rabbitmq.WithConsumeOptionsBindingExchangeKind("topic"),
		rabbitmq.WithConsumeOptionsBindingExchangeDurable,
	)
	if err != nil {
		log.Fatal(err)
	}

	// block main thread so consumers run forever
	forever := make(chan struct{})
	<-forever
}
