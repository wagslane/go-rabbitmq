package rabbitmq

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
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
	chManager *channelManager
	logger    Logger
}

// ConsumerOptions are used to describe a consumer's configuration.
// Logging set to true will enable the consumer to print to stdout
// Logger specifies a custom Logger interface implementation overruling Logging.
type ConsumerOptions struct {
	Logging bool
	Logger  Logger
}

// Delivery captures the fields for a previously delivered message resident in
// a queue to be delivered by the server to a consumer from Channel.Consume or
// Channel.Get.
type Delivery struct {
	amqp.Delivery
}

// NewConsumer returns a new Consumer connected to the given rabbitmq server
func NewConsumer(url string, config amqp.Config, optionFuncs ...func(*ConsumerOptions)) (Consumer, error) {
	options := &ConsumerOptions{}
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}
	if options.Logger == nil {
		options.Logger = &noLogger{} // default no logging
	}

	chManager, err := newChannelManager(url, config, options.Logger)
	if err != nil {
		return Consumer{}, err
	}
	consumer := Consumer{
		chManager: chManager,
		logger:    options.Logger,
	}
	return consumer, nil
}

// WithConsumerOptionsLogging sets a logger to log to stdout
func WithConsumerOptionsLogging(options *ConsumerOptions) {
	options.Logging = true
	options.Logger = &stdLogger{}
}

// WithConsumerOptionsLogger sets logging to a custom interface.
// Use WithConsumerOptionsLogging to just log to stdout.
func WithConsumerOptionsLogger(log Logger) func(options *ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.Logging = true
		options.Logger = log
	}
}

// StartConsuming starts n goroutines where n="ConsumeOptions.QosOptions.Concurrency".
// Each goroutine spawns a handler that consumes off of the qiven queue which binds to the routing key(s).
// The provided handler is called once for each message. If the provided queue doesn't exist, it
// will be created on the cluster
func (consumer Consumer) StartConsuming(
	handler Handler,
	queue string,
	routingKeys []string,
	optionFuncs ...func(*ConsumeOptions),
) error {
	defaultOptions := getDefaultConsumeOptions()
	options := &defaultOptions
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	err := consumer.startGoroutines(
		handler,
		queue,
		routingKeys,
		*options,
	)
	if err != nil {
		return err
	}

	go func() {
		for err := range consumer.chManager.notifyCancelOrClose {
			consumer.logger.Printf("consume cancel/close handler triggered. err: %v", err)
			consumer.startGoroutinesWithRetries(
				handler,
				queue,
				routingKeys,
				*options,
			)
		}
	}()
	return nil
}

// Disconnect disconnects both the channel and the connection.
// This method doesn't throw a reconnect, and should be used when finishing a program.
// IMPORTANT: If this method is executed before StopConsuming, it could cause unexpected behavior
// such as messages being processed, but not being acknowledged, thus being requeued by the broker
func (consumer Consumer) Disconnect() {
	consumer.chManager.channel.Close()
	consumer.chManager.connection.Close()
}

// StopConsuming stops the consumption of messages.
// The consumer should be discarded as it's not safe for re-use.
// This method sends a basic.cancel notification.
// The consumerName is the name or delivery tag of the amqp consumer we want to cancel.
// When noWait is true, do not wait for the server to acknowledge the cancel.
// Only use this when you are certain there are no deliveries in flight that
// require an acknowledgment, otherwise they will arrive and be dropped in the
// client without an ack, and will not be redelivered to other consumers.
// IMPORTANT: Since the streadway library doesn't provide a way to retrieve the consumer's tag after the creation
// it's imperative for you to set the name when creating the consumer, if you want to use this function later
// a simple uuid4 should do the trick, since it should be unique.
// If you start many consumers, you should store the name of the consumers when creating them, such that you can
// use them in a for to stop all the consumers.
func (consumer Consumer) StopConsuming(consumerName string, noWait bool) {
	consumer.chManager.channel.Cancel(consumerName, noWait)
}

// startGoroutinesWithRetries attempts to start consuming on a channel
// with an exponential backoff
func (consumer Consumer) startGoroutinesWithRetries(
	handler Handler,
	queue string,
	routingKeys []string,
	consumeOptions ConsumeOptions,
) {
	backoffTime := time.Second
	for {
		consumer.logger.Printf("waiting %s seconds to attempt to start consumer goroutines", backoffTime)
		time.Sleep(backoffTime)
		backoffTime *= 2
		err := consumer.startGoroutines(
			handler,
			queue,
			routingKeys,
			consumeOptions,
		)
		if err != nil {
			consumer.logger.Printf("couldn't start consumer goroutines. err: %v", err)
			continue
		}
		break
	}
}

// startGoroutines declares the queue if it doesn't exist,
// binds the queue to the routing key(s), and starts the goroutines
// that will consume from the queue
func (consumer Consumer) startGoroutines(
	handler Handler,
	queue string,
	routingKeys []string,
	consumeOptions ConsumeOptions,
) error {
	consumer.chManager.channelMux.RLock()
	defer consumer.chManager.channelMux.RUnlock()

	if consumeOptions.QueueDeclare {
		_, err := consumer.chManager.channel.QueueDeclare(
			queue,
			consumeOptions.QueueDurable,
			consumeOptions.QueueAutoDelete,
			consumeOptions.QueueExclusive,
			consumeOptions.QueueNoWait,
			tableToAMQPTable(consumeOptions.QueueArgs),
		)
		if err != nil {
			return err
		}
	}

	if consumeOptions.BindingExchange != nil {
		exchange := consumeOptions.BindingExchange
		if exchange.Name == "" {
			return fmt.Errorf("binding to exchange but name not specified")
		}
		if exchange.Declare {
			err := consumer.chManager.channel.ExchangeDeclare(
				exchange.Name,
				exchange.Kind,
				exchange.Durable,
				exchange.AutoDelete,
				exchange.Internal,
				exchange.NoWait,
				tableToAMQPTable(exchange.ExchangeArgs),
			)
			if err != nil {
				return err
			}
		}
		for _, routingKey := range routingKeys {
			err := consumer.chManager.channel.QueueBind(
				queue,
				routingKey,
				exchange.Name,
				consumeOptions.BindingNoWait,
				tableToAMQPTable(consumeOptions.BindingArgs),
			)
			if err != nil {
				return err
			}
		}
	}

	err := consumer.chManager.channel.Qos(
		consumeOptions.QOSPrefetch,
		0,
		consumeOptions.QOSGlobal,
	)
	if err != nil {
		return err
	}

	msgs, err := consumer.chManager.channel.Consume(
		queue,
		consumeOptions.ConsumerName,
		consumeOptions.ConsumerAutoAck,
		consumeOptions.ConsumerExclusive,
		consumeOptions.ConsumerNoLocal, // no-local is not supported by RabbitMQ
		consumeOptions.ConsumerNoWait,
		tableToAMQPTable(consumeOptions.ConsumerArgs),
	)
	if err != nil {
		return err
	}

	for i := 0; i < consumeOptions.Concurrency; i++ {
		go func() {
			for msg := range msgs {
				if consumeOptions.ConsumerAutoAck {
					handler(Delivery{msg})
					continue
				}
				switch handler(Delivery{msg}) {
				case Ack:
					err := msg.Ack(false)
					if err != nil {
						consumer.logger.Printf("can't ack message: %v", err)
					}
				case NackDiscard:
					err := msg.Nack(false, false)
					if err != nil {
						consumer.logger.Printf("can't nack message: %v", err)
					}
				case NackRequeue:
					err := msg.Nack(false, true)
					if err != nil {
						consumer.logger.Printf("can't nack message: %v", err)
					}
				}
			}
			consumer.logger.Printf("rabbit consumer goroutine closed")
		}()
	}
	consumer.logger.Printf("Processing messages on %v goroutines", consumeOptions.Concurrency)
	return nil
}
