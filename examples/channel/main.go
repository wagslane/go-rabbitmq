package main

import (
    "log"

    "github.com/rabbitmq/amqp091-go"
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

    channel, err := rabbitmq.NewChannel(conn)
    if err != nil {
        log.Fatal(err)
    }
    defer channel.Close()

    table := amqp091.Table{
        "x-dead-letter-exchange":    "events",
        "x-dead-letter-routing-key": "my_routing_key",
    }
    _, err = channel.QueueDeclareSafe("re_my_routing_key", true, false, false, false, table)
    if err != nil {
        log.Fatal(err)
    }
    err = channel.QueueBindSafe("re_my_queue", "re_my_routing_key", "events", false, table)
    if err != nil {
        log.Fatal(err)
    }
}
