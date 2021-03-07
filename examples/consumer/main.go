package main

import (
	"log"

	rabbitmq "github.com/wagslane/go-rabbitmq"
)

func main() {
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

	// block main thread so consumers run forever
	forever := make(chan struct{})
	<-forever
}
