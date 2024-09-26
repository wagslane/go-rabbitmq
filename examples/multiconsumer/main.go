package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	rabbitmq "github.com/makometr/go-rabbitmq"
)

func main() {
	conn, err := rabbitmq.NewConn(
		"amqp://guest:guest@localhost",
		rabbitmq.WithConnectionOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	consumer, err := rabbitmq.NewConsumer(
		conn,
		"my_queue",
		rabbitmq.WithConsumerOptionsConcurrency(2),
		rabbitmq.WithConsumerOptionsConsumerName("consumer_1"),
		rabbitmq.WithConsumerOptionsRoutingKey("my_routing_key"),
		rabbitmq.WithConsumerOptionsRoutingKey("my_routing_key_2"),
		rabbitmq.WithConsumerOptionsExchangeName("events"),
		rabbitmq.WithConsumerOptionsExchangeDeclare,
	)
	if err != nil {
		log.Fatal(err)
	}

	consumer2, err := rabbitmq.NewConsumer(
		conn,
		"my_queue",
		rabbitmq.WithConsumerOptionsConcurrency(2),
		rabbitmq.WithConsumerOptionsConsumerName("consumer_2"),
		rabbitmq.WithConsumerOptionsRoutingKey("my_routing_key"),
		rabbitmq.WithConsumerOptionsExchangeName("events"),
	)
	if err != nil {
		log.Fatal(err)
	}

	sigs := make(chan os.Signal, 1)
	errs := make(chan error, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		fmt.Println("awaiting signal")
		select {
		case sig := <-sigs:
			fmt.Println()
			fmt.Println(sig)
		case err := <-errs:
			log.Print(err)
		}

		fmt.Println("stopping consumers")

		consumer.Close()
		consumer2.Close()
	}()

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		err := consumer.Run(func(d rabbitmq.Delivery) rabbitmq.Action {
			log.Printf("consumed: %v", string(d.Body))
			// rabbitmq.Ack, rabbitmq.NackDiscard, rabbitmq.NackRequeue
			return rabbitmq.Ack
		})
		if err != nil {
			errs <- err
		}
	}()

	go func() {
		defer wg.Done()

		err := consumer2.Run(func(d rabbitmq.Delivery) rabbitmq.Action {
			log.Printf("consumed: %v", string(d.Body))
			// rabbitmq.Ack, rabbitmq.NackDiscard, rabbitmq.NackRequeue
			return rabbitmq.Ack
		})
		if err != nil {
			errs <- err
		}
	}()

	wg.Wait()
}
