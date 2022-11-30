package rabbitmq

import (
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/wagslane/go-rabbitmq/internal/connectionmanager"
	"github.com/wagslane/go-rabbitmq/internal/logger"
)

// Action is an action that occurs after processed this delivery
type Action int

// Handler defines the handler of each Delivery and return Action
type Handler func(d Delivery) (action Action)

const (
	// Ack default ack this msg after you have successfully processed this delivery.
	Ack Action = iota
	// NackDiscard the message will be dropped or delivered to a server configured dead-letter queue.
	NackDiscard
	// NackRequeue deliver this message to a different consumer.
	NackRequeue
)

// Consumer allows you to create and connect to queues for data consumption.
type Consumer struct {
	connManager                *connectionmanager.ConnectionManager
	reconnectErrCh             <-chan error
	closeConnectionToManagerCh chan<- struct{}
	options                    ConsumerOptions
}

// ConsumerOptions are used to describe a consumer's configuration.
// Logger specifies a custom Logger interface implementation.
type ConsumerOptions struct {
	Logger logger.Logger
}

// Delivery captures the fields for a previously delivered message resident in
// a queue to be delivered by the server to a consumer from Channel.Consume or
// Channel.Get.
type Delivery struct {
	amqp.Delivery
}

// NewConsumer returns a new Consumer connected to the given rabbitmq server
func NewConsumer(conn *Conn, optionFuncs ...func(*ConsumerOptions)) (*Consumer, error) {
	options := &ConsumerOptions{
		Logger: &stdDebugLogger{},
	}
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	if conn.connectionManager == nil {
		return nil, errors.New("connection manager can't be nil")
	}
	reconnectErrCh, closeCh := conn.connectionManager.NotifyReconnect()

	consumer := &Consumer{
		connManager:                conn.connectionManager,
		reconnectErrCh:             reconnectErrCh,
		closeConnectionToManagerCh: closeCh,
		options:                    *options,
	}

	return consumer, nil
}

// WithConsumerOptionsLogging uses a default logger that writes to std out
func WithConsumerOptionsLogging(options *ConsumerOptions) {
	options.Logger = &stdDebugLogger{}
}

// WithConsumerOptionsLogger sets logging to a custom interface.
// Use WithConsumerOptionsLogging to just log to stdout.
func WithConsumerOptionsLogger(log logger.Logger) func(options *ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.Logger = log
	}
}

// StartConsuming starts n goroutines where n="ConsumeOptions.QosOptions.Concurrency".
// Each goroutine spawns a handler that consumes off of the given queue which binds to the routing key(s).
// The provided handler is called once for each message. If the provided queue doesn't exist, it
// will be created on the cluster
func (consumer *Consumer) StartConsuming(
	handler Handler,
	queue string,
	optionFuncs ...func(*ConsumeOptions),
) error {
	defaultOptions := getDefaultConsumeOptions(queue)
	options := &defaultOptions
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	err := consumer.startGoroutines(
		handler,
		*options,
	)
	if err != nil {
		return err
	}

	go func() {
		for err := range consumer.reconnectErrCh {
			consumer.options.Logger.Infof("successful recovery from: %v", err)
			err = consumer.startGoroutines(
				handler,
				*options,
			)
			if err != nil {
				consumer.options.Logger.Errorf("error restarting consumer goroutines after cancel or close: %v", err)
			}
		}
	}()
	return nil
}

// Close cleans up resources and closes the consumer.
// It does not close the connection manager, just the subscription
// to the connection manager
func (consumer *Consumer) Close() {
	consumer.options.Logger.Infof("closing consumer...")
	consumer.closeConnectionToManagerCh <- struct{}{}
}

// startGoroutines declares the queue if it doesn't exist,
// binds the queue to the routing key(s), and starts the goroutines
// that will consume from the queue
func (consumer *Consumer) startGoroutines(
	handler Handler,
	options ConsumeOptions,
) error {

	err := declareExchange(consumer.connManager, options.ExchangeOptions)
	if err != nil {
		return fmt.Errorf("declare exchange failed: %w", err)
	}
	err = declareQueue(consumer.connManager, options.QueueOptions)
	if err != nil {
		return fmt.Errorf("declare queue failed: %w", err)
	}
	err = declareBindings(consumer.connManager, options)
	if err != nil {
		return fmt.Errorf("declare bindings failed: %w", err)
	}

	msgs, err := consumer.connManager.ConsumeSafe(
		options.QueueOptions.Name,
		options.RabbitConsumerOptions.Name,
		options.RabbitConsumerOptions.AutoAck,
		options.RabbitConsumerOptions.Exclusive,
		false, // no-local is not supported by RabbitMQ
		options.RabbitConsumerOptions.NoWait,
		tableToAMQPTable(options.RabbitConsumerOptions.Args),
	)
	if err != nil {
		return err
	}

	for i := 0; i < options.Concurrency; i++ {
		go handlerGoroutine(consumer, msgs, options, handler)
	}
	consumer.options.Logger.Infof("Processing messages on %v goroutines", options.Concurrency)
	return nil
}

func handlerGoroutine(consumer *Consumer, msgs <-chan amqp.Delivery, consumeOptions ConsumeOptions, handler Handler) {
	for msg := range msgs {
		if consumeOptions.RabbitConsumerOptions.AutoAck {
			handler(Delivery{msg})
			continue
		}
		switch handler(Delivery{msg}) {
		case Ack:
			err := msg.Ack(false)
			if err != nil {
				consumer.options.Logger.Errorf("can't ack message: %v", err)
			}
		case NackDiscard:
			err := msg.Nack(false, false)
			if err != nil {
				consumer.options.Logger.Errorf("can't nack message: %v", err)
			}
		case NackRequeue:
			err := msg.Nack(false, true)
			if err != nil {
				consumer.options.Logger.Errorf("can't nack message: %v", err)
			}
		}
	}
	consumer.options.Logger.Infof("rabbit consumer goroutine closed")
}
