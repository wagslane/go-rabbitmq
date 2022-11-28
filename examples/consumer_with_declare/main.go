package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	rabbitmq "github.com/wagslane/go-rabbitmq"
)

var consumerName = "example_with_declare"

func main() {
	consumer, err := rabbitmq.NewConsumer(
		"amqp://guest:guest@localhost", rabbitmq.Config{},
		rabbitmq.WithConsumerOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := consumer.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	err = consumer.StartConsuming(
		func(d rabbitmq.Delivery) rabbitmq.Action {
			log.Printf("consumed: %v", string(d.Body))
			// rabbitmq.Ack, rabbitmq.NackDiscard, rabbitmq.NackRequeue
			return rabbitmq.Ack
		},
		"my_queue",
		rabbitmq.WithConsumeDeclareOptions(
			rabbitmq.WithDeclareQueueDurable,
			rabbitmq.WithDeclareQueueQuorum,
			rabbitmq.WithDeclareExchangeName("events"),
			rabbitmq.WithDeclareExchangeKind("topic"),
			rabbitmq.WithDeclareExchangeDurable,
			rabbitmq.WithDeclareBindingsForRoutingKeys([]string{"routing_key", "routing_key_2"}), // implicit bindings
			rabbitmq.WithDeclareBindings([]rabbitmq.Binding{ // custom bindings
				{
					QueueName:    "my_queue",
					ExchangeName: "events",
					RoutingKey:   "a_custom_key",
				},
			}),
		),
		rabbitmq.WithConsumeOptionsConsumerName(consumerName),
	)
	if err != nil {
		log.Fatal(err)
	}

	// block main thread - wait for shutdown signal
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		fmt.Println()
		fmt.Println(sig)
		done <- true
	}()

	fmt.Println("awaiting signal")
	<-done
	fmt.Println("stopping consumer")
}
