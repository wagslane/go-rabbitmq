package rabbitmq

import (
	"time"

	"github.com/streadway/amqp"
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
func NewConsumer(url string, optionFuncs ...func(*ConsumerOptions)) (Consumer, error) {
	options := &ConsumerOptions{}
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}
	if options.Logger == nil {
		options.Logger = &nolog{} // default no logging
	}

	chManager, err := newChannelManager(url, options.Logger)
	if err != nil {
		return Consumer{}, err
	}
	consumer := Consumer{
		chManager: chManager,
		logger:    options.Logger,
	}
	return consumer, nil
}

// WithConsumerOptionsLogging sets logging to true on the consumer options
func WithConsumerOptionsLogging(options *ConsumerOptions) {
	options.Logging = true
	options.Logger = &stdlog{}
}

// WithConsumerOptionsLogger sets logging to a custom interface.
// Use WithConsumerOptionsLogging to just log to stdout.
func WithConsumerOptionsLogger(log Logger) func(options *ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.Logging = true
		options.Logger = log
	}
}

// getDefaultConsumeOptions descibes the options that will be used when a value isn't provided
func getDefaultConsumeOptions() ConsumeOptions {
	return ConsumeOptions{
		QueueDurable:      false,
		QueueAutoDelete:   false,
		QueueExclusive:    false,
		QueueNoWait:       false,
		QueueArgs:         nil,
		BindingExchange:   "",
		BindingNoWait:     false,
		BindingArgs:       nil,
		Concurrency:       1,
		QOSPrefetch:       0,
		QOSGlobal:         false,
		ConsumerName:      "",
		ConsumerAutoAck:   false,
		ConsumerExclusive: false,
		ConsumerNoWait:    false,
		ConsumerNoLocal:   false,
		ConsumerArgs:      nil,
	}
}

// ConsumeOptions are used to describe how a new consumer will be created.
type ConsumeOptions struct {
	QueueDurable      bool
	QueueAutoDelete   bool
	QueueExclusive    bool
	QueueNoWait       bool
	QueueArgs         Table
	BindingExchange   string
	BindingNoWait     bool
	BindingArgs       Table
	Concurrency       int
	QOSPrefetch       int
	QOSGlobal         bool
	ConsumerName      string
	ConsumerAutoAck   bool
	ConsumerExclusive bool
	ConsumerNoWait    bool
	ConsumerNoLocal   bool
	ConsumerArgs      Table
}

// WithConsumeOptionsQueueDurable sets the queue to durable, which means it won't
// be destroyed when the server restarts. It must only be bound to durable exchanges
func WithConsumeOptionsQueueDurable(options *ConsumeOptions) {
	options.QueueDurable = true
}

// WithConsumeOptionsQueueAutoDelete sets the queue to auto delete, which means it will
// be deleted when there are no more conusmers on it
func WithConsumeOptionsQueueAutoDelete(options *ConsumeOptions) {
	options.QueueAutoDelete = true
}

// WithConsumeOptionsQueueExclusive sets the queue to exclusive, which means
// it's are only accessible by the connection that declares it and
// will be deleted when the connection closes. Channels on other connections
// will receive an error when attempting to declare, bind, consume, purge or
// delete a queue with the same name.
func WithConsumeOptionsQueueExclusive(options *ConsumeOptions) {
	options.QueueExclusive = true
}

// WithConsumeOptionsQueueNoWait sets the queue to nowait, which means
// the queue will assume to be declared on the server.  A
// channel exception will arrive if the conditions are met for existing queues
// or attempting to modify an existing queue from a different connection.
func WithConsumeOptionsQueueNoWait(options *ConsumeOptions) {
	options.QueueNoWait = true
}

// WithConsumeOptionsQuorum sets the queue a quorum type, which means multiple nodes
// in the cluster will have the messages distributed amongst them for higher reliability
func WithConsumeOptionsQuorum(options *ConsumeOptions) {
	if options.QueueArgs == nil {
		options.QueueArgs = Table{}
	}
	options.QueueArgs["x-queue-type"] = "quorum"
}

// WithConsumeOptionsBindingExchange returns a function that sets the exchange the queue will be bound to
func WithConsumeOptionsBindingExchange(exchange string) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		options.BindingExchange = exchange
	}
}

// WithConsumeOptionsBindingNoWait sets the bindings to nowait, which means if the queue can not be bound
// the channel will not be closed with an error.
func WithConsumeOptionsBindingNoWait(options *ConsumeOptions) {
	options.BindingNoWait = true
}

// WithConsumeOptionsConcurrency returns a function that sets the concurrency, which means that
// many goroutines will be spawned to run the provided handler on messages
func WithConsumeOptionsConcurrency(concurrency int) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		options.Concurrency = concurrency
	}
}

// WithConsumeOptionsQOSPrefetch returns a function that sets the prefetch count, which means that
// many messages will be fetched from the server in advance to help with throughput.
// This doesn't affect the handler, messages are still processed one at a time.
func WithConsumeOptionsQOSPrefetch(prefetchCount int) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		options.QOSPrefetch = prefetchCount
	}
}

// WithConsumeOptionsQOSGlobal sets the qos on the channel to global, which means
// these QOS settings apply to ALL existing and future
// consumers on all channels on the same connection
func WithConsumeOptionsQOSGlobal(options *ConsumeOptions) {
	options.QOSGlobal = true
}

// WithConsumeOptionsConsumerName returns a function that sets the name on the server of this consumer
// if unset a random name will be given
func WithConsumeOptionsConsumerName(consumerName string) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		options.ConsumerName = consumerName
	}
}

// WithConsumeOptionsConsumerExclusive sets the consumer to exclusive, which means
// the server will ensure that this is the sole consumer
// from this queue. When exclusive is false, the server will fairly distribute
// deliveries across multiple consumers.
func WithConsumeOptionsConsumerExclusive(options *ConsumeOptions) {
	options.ConsumerExclusive = true
}

// WithConsumeOptionsConsumerNoWait sets the consumer to nowait, which means
// it does not wait for the server to confirm the request and
// immediately begin deliveries. If it is not possible to consume, a channel
// exception will be raised and the channel will be closed.
func WithConsumeOptionsConsumerNoWait(options *ConsumeOptions) {
	options.ConsumerNoWait = true
}

// StartConsuming starts n goroutines where n="ConsumeOptions.QosOptions.Concurrency".
// Each goroutine spawns a handler that consumes off of the qiven queue which binds to the routing key(s).
// The provided handler is called once for each message. If the provided queue doesn't exist, it
// will be created on the cluster
func (consumer Consumer) StartConsuming(
	handler func(d Delivery) bool,
	queue string,
	routingKeys []string,
	optionFuncs ...func(*ConsumeOptions),
) error {
	defaultOptions := getDefaultConsumeOptions()
	options := &ConsumeOptions{}
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}
	if options.Concurrency < 1 {
		options.Concurrency = defaultOptions.Concurrency
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

// startGoroutinesWithRetries attempts to start consuming on a channel
// with an exponential backoff
func (consumer Consumer) startGoroutinesWithRetries(
	handler func(d Delivery) bool,
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
	handler func(d Delivery) bool,
	queue string,
	routingKeys []string,
	consumeOptions ConsumeOptions,
) error {
	consumer.chManager.channelMux.RLock()
	defer consumer.chManager.channelMux.RUnlock()

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

	if consumeOptions.BindingExchange != "" {
		for _, routingKey := range routingKeys {
			err = consumer.chManager.channel.QueueBind(
				queue,
				routingKey,
				consumeOptions.BindingExchange,
				consumeOptions.BindingNoWait,
				tableToAMQPTable(consumeOptions.BindingArgs),
			)
			if err != nil {
				return err
			}
		}
	}

	err = consumer.chManager.channel.Qos(
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
				if handler(Delivery{msg}) {
					err := msg.Ack(false)
					if err != nil {
						consumer.logger.Printf("can't ack message: %v", err)
					}
				} else {
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
