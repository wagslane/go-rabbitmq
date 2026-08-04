package main

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	rabbitmq "github.com/wagslane/go-rabbitmq"
)

// deleteQueues removes leftovers from earlier runs so the example starts from
// a known state: no_such_queue must not exist for the unroutable simulation
// to hold, and my_queue may still hold unconsumed messages. go-rabbitmq has
// no queue admin API, so this uses the underlying amqp client. Deleting a
// queue that doesn't exist is a no-op.
func deleteQueues(url string, names ...string) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	for _, name := range names {
		if _, err := ch.QueueDelete(name, false, false, false); err != nil {
			return err
		}
	}
	return nil
}

// message is this example's own record of what it published. It is attached to
// each publishing with WithPublishOptionsOutcomeRef and handed back on the
// Outcome, which is what makes the retry loop below possible: an Outcome on its
// own carries no payload unless the broker returned the message.
type message struct {
	id         int
	body       []byte
	routingKey string
}

// This example publishes a batch of messages and republishes the ones that
// failed: nacked, returned as unroutable, or lost to a reconnect.
// Confirmations and returns are paired per message by the library, so the
// failed set is exact even with the whole batch in flight: expect the 50
// messages aimed at no_such_queue to fail and the other 450 to be delivered.
// Each failure identifies itself, so the retry needs no bookkeeping tying
// outcomes back to payloads.
func main() {
	const url = "amqp://guest:guest@localhost"

	if err := deleteQueues(url, "no_such_queue", "my_queue"); err != nil {
		log.Fatal(err)
	}

	conn, err := rabbitmq.NewConn(
		url,
		rabbitmq.WithConnectionOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// the consumer declares my_queue and drains it; without the queue every
	// message would be unroutable, not just the no_such_queue ones
	// durable: recent RabbitMQ versions refuse transient non-exclusive queues
	consumer, err := rabbitmq.NewConsumer(conn, "my_queue",
		rabbitmq.WithConsumerOptionsLogging,
		rabbitmq.WithConsumerOptionsQueueDurable,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	go func() {
		if err := consumer.Run(func(d rabbitmq.Delivery) rabbitmq.Action {
			return rabbitmq.Ack
		}); err != nil {
			log.Println("consumer stopped:", err)
		}
	}()
	// give the consumer a moment to declare the queue before publishing
	time.Sleep(time.Second)

	publisher, err := rabbitmq.NewPublisher(
		conn,
		rabbitmq.WithPublisherOptionsLogging,
		rabbitmq.WithPublisherOptionsConfirm,
		// bounds memory: at most 64 messages awaiting their outcome at once
		rabbitmq.WithPublisherOptionsMaxOutcomesInFlight(64),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer publisher.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var outcomes []*rabbitmq.PublishOutcome
	for i := range 500 {
		msg := &message{id: i, body: []byte(fmt.Sprintf("message %d", i)), routingKey: "my_queue"}
		if i%10 == 0 {
			msg.routingKey = "no_such_queue" // simulate unroutable messages
		}
		batch, err := publisher.PublishWithOutcome(
			ctx,
			msg.body,
			[]string{msg.routingKey},
			// mandatory is required for the broker to return unroutable
			// messages instead of silently dropping them
			rabbitmq.WithPublishOptionsMandatory,
			// hands msg back on the outcome, so a failure knows what it was
			rabbitmq.WithPublishOptionsOutcomeRef(msg),
		)
		if err != nil {
			log.Fatalf("publish %d: %v", i, err)
		}
		outcomes = append(outcomes, batch...)
	}

	// the failed outcomes can be collected and carried around on their own:
	// each one still identifies its message
	var failed []rabbitmq.Outcome
	for _, po := range outcomes {
		outcome, err := po.Wait(ctx)
		if err != nil {
			log.Fatalf("waiting for outcomes: %v", err)
		}
		if outcome.Failed() {
			failed = append(failed, outcome)
		}
	}

	log.Printf("%d of %d messages need to be republished", len(failed), len(outcomes))

	var retries []*rabbitmq.PublishOutcome
	for _, outcome := range failed {
		msg := outcome.Ref.(*message)
		routingKey := msg.routingKey
		switch {
		case outcome.Err != nil:
			// channel was lost mid-flight: the message may have been
			// delivered, so republishing it can duplicate it
			log.Printf("message %d: outcome unknown (%s), republishing anyway", msg.id, outcome.ID)
		case outcome.Return != nil:
			// the same routing key would be unroutable again, so a real app
			// would send it somewhere that exists or park it for inspection
			log.Printf("message %d: unroutable (%s), rerouting to my_queue", msg.id, outcome.Return.ReplyText)
			routingKey = "my_queue"
		default:
			log.Printf("message %d: nacked by the broker, republishing", msg.id)
		}

		batch, err := publisher.PublishWithOutcome(
			ctx,
			msg.body,
			[]string{routingKey},
			rabbitmq.WithPublishOptionsExchange(outcome.Exchange),
			rabbitmq.WithPublishOptionsMandatory,
			rabbitmq.WithPublishOptionsOutcomeRef(msg),
		)
		if err != nil {
			log.Fatalf("republish of message %d: %v", msg.id, err)
		}
		retries = append(retries, batch...)
	}

	stillFailed := 0
	for _, po := range retries {
		outcome, err := po.Wait(ctx)
		if err != nil {
			log.Fatalf("waiting for retry outcomes: %v", err)
		}
		if outcome.Failed() {
			stillFailed++
			log.Printf("message %d still failed after retry: %+v", outcome.Ref.(*message).id, outcome)
		}
	}
	log.Printf("republished %d messages, %d still failed", len(retries), stillFailed)
}
