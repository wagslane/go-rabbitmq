package main

import (
	"log"

	"github.com/rabbitmq/amqp091-go"
	"github.com/xmapst/go-rabbitmq"
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

	channel, err := rabbitmq.NewChannel(conn)
	if err != nil {
		log.Fatal(err)
	}
	defer channel.Close()

	_, err = channel.QueueDeclareSafe(
		"re_my_queue", true, false, false, false,
		amqp091.Table{
			"x-dead-letter-exchange":    "events",
			"x-dead-letter-routing-key": "my_routing_key",
		})
	if err != nil {
		log.Fatal(err)
	}

	err = channel.QueueBindSafe("re_my_routing_key", "re_my_queue", "events", false, amqp091.Table{})
	if err != nil {
		log.Fatal(err)
	}
}
