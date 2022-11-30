package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	rabbitmq "github.com/wagslane/go-rabbitmq"
)

var consumerName = "example"

func main() {
	conn, err := rabbitmq.NewConn(
		"amqp://guest:guest@localhost",
		rabbitmq.WithConnectionOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}

	consumer, err := rabbitmq.NewConsumer(
		conn,
		rabbitmq.WithConsumerOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	err = consumer.StartConsuming(
		func(d rabbitmq.Delivery) rabbitmq.Action {
			log.Printf("consumed: %v", string(d.Body))
			// rabbitmq.Ack, rabbitmq.NackDiscard, rabbitmq.NackRequeue
			return rabbitmq.Ack
		},
		"my_queue",
		rabbitmq.WithConsumeOptionsConcurrency(2),
		rabbitmq.WithConsumeOptionsConsumerName(consumerName),
		rabbitmq.WithConsumeOptionsDefaultBinding("my_routing_key"),
		rabbitmq.WithConsumeOptionsExchangeName("events"),
	)
	if err != nil {
		log.Fatal(err)
	}

	consumer2, err := rabbitmq.NewConsumer(
		conn,
		rabbitmq.WithConsumerOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer2.Close()

	err = consumer2.StartConsuming(
		func(d rabbitmq.Delivery) rabbitmq.Action {
			log.Printf("consumed 2: %v", string(d.Body))
			// rabbitmq.Ack, rabbitmq.NackDiscard, rabbitmq.NackRequeue
			return rabbitmq.Ack
		},
		"my_queue_2",
		rabbitmq.WithConsumeOptionsConcurrency(2),
		rabbitmq.WithConsumeOptionsConsumerName("consumer3"),
		rabbitmq.WithConsumeOptionsDefaultBinding("my_routing_key"),
		rabbitmq.WithConsumeOptionsExchangeName("events"),
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
