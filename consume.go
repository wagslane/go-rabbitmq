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
// Logger specifies a custom Logger interface implementation.
type ConsumerOptions struct {
	Logger            Logger
	ReconnectInterval time.Duration
}

// Delivery captures the fields for a previously delivered message resident in
// a queue to be delivered by the server to a consumer from Channel.Consume or
// Channel.Get.
type Delivery struct {
	amqp.Delivery
}

// NewConsumer returns a new Consumer connected to the given rabbitmq server
func NewConsumer(url string, config Config, optionFuncs ...func(*ConsumerOptions)) (Consumer, error) {
	options := &ConsumerOptions{
		Logger:            &stdDebugLogger{},
		ReconnectInterval: time.Second * 5,
	}
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	chManager, err := newChannelManager(url, config, options.Logger, options.ReconnectInterval)
	if err != nil {
		return Consumer{}, err
	}
	consumer := Consumer{
		chManager: chManager,
		logger:    options.Logger,
	}
	return consumer, nil
}

// WithConsumerOptionsReconnectInterval sets the interval at which the consumer will
// attempt to reconnect to the rabbit server
func WithConsumerOptionsReconnectInterval(reconnectInterval time.Duration) func(options *ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.ReconnectInterval = reconnectInterval
	}
}

// WithConsumerOptionsLogging uses a default logger that writes to std out
func WithConsumerOptionsLogging(options *ConsumerOptions) {
	options.Logger = &stdDebugLogger{}
}

// WithConsumerOptionsLogger sets logging to a custom interface.
// Use WithConsumerOptionsLogging to just log to stdout.
func WithConsumerOptionsLogger(log Logger) func(options *ConsumerOptions) {
	return func(options *ConsumerOptions) {
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
			consumer.logger.Infof("successful recovery from: %v", err)
			err = consumer.startGoroutines(
				handler,
				queue,
				routingKeys,
				*options,
			)
			if err != nil {
				consumer.logger.Errorf("error restarting consumer goroutines after cancel or close: %v", err)
			}
		}
	}()
	return nil
}

// Close cleans up resources and closes the consumer.
// The consumer is not safe for reuse
func (consumer Consumer) Close() error {
	consumer.chManager.logger.Infof("closing consumer...")
	return consumer.chManager.close()
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
		go handlerGoroutine(consumer, msgs, consumeOptions, handler)
	}
	consumer.logger.Infof("Processing messages on %v goroutines", consumeOptions.Concurrency)
	return nil
}

func handlerGoroutine(consumer Consumer, msgs <-chan amqp.Delivery, consumeOptions ConsumeOptions, handler Handler) {
	for msg := range msgs {
		if consumeOptions.ConsumerAutoAck {
			handler(Delivery{msg})
			continue
		}
		switch handler(Delivery{msg}) {
		case Ack:
			err := msg.Ack(false)
			if err != nil {
				consumer.logger.Errorf("can't ack message: %v", err)
			}
		case NackDiscard:
			err := msg.Nack(false, false)
			if err != nil {
				consumer.logger.Errorf("can't nack message: %v", err)
			}
		case NackRequeue:
			err := msg.Nack(false, true)
			if err != nil {
				consumer.logger.Errorf("can't nack message: %v", err)
			}
		}
	}
	consumer.logger.Infof("rabbit consumer goroutine closed")
}
