package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/wagslane/go-rabbitmq"
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

	consumer, err := rabbitmq.NewSuperConsumer(
		conn,
		func(d rabbitmq.Delivery) (action rabbitmq.Action) {
			log.Printf("consumed: %v", string(d.Body))
			return rabbitmq.Ack
		},
		"my_queue",
		rabbitmq.WithSuperConsumerOptionsExchangeName("exchange1"),
		rabbitmq.WithSuperConsumerOptionsRoutingKey("routingKey1", "exchange1"),
		rabbitmq.WithSuperConsumerOptionsRoutingKey("routingKey2", "exchange1"),
		rabbitmq.WithSuperConsumerOptionsExchangeDeclare("exchange1"),
		rabbitmq.WithSuperConsumerOptionsExchangeDurable("exchange1"),
		rabbitmq.WithSuperConsumerOptionsExchangeName("exchange2"),
		rabbitmq.WithSuperConsumerOptionsRoutingKey("routingKey3", "exchange2"),
		rabbitmq.WithSuperConsumerOptionsExchangeDeclare("exchange2"),
		rabbitmq.WithSuperConsumerOptionsExchangeDurable("exchange2"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

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
