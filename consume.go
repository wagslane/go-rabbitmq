package rabbitmq

import (
	"time"

	"github.com/streadway/amqp"
)

// Consumer allows you to create and connect to queues for data consumption.
type Consumer struct {
	chManager *channelManager
	logger    logger
}

// ConsumerOptions are used to describe a consumer's configuration.
// Logging set to true will enable the consumer to print to stdout
type ConsumerOptions struct {
	Logging bool
}

// Delivery captures the fields for a previously delivered message resident in
// a queue to be delivered by the server to a consumer from Channel.Consume or
// Channel.Get.
type Delivery struct {
	amqp.Delivery
}

// GetConsumer returns a new Consumer connected to the given rabbitmq server
func GetConsumer(url string, optionFuncs ...func(*ConsumerOptions)) (Consumer, error) {
	options := &ConsumerOptions{}
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	chManager, err := newChannelManager(url, options.Logging)
	if err != nil {
		return Consumer{}, err
	}
	consumer := Consumer{
		chManager: chManager,
		logger:    logger{logging: options.Logging},
	}
	return consumer, nil
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
					msg.Ack(false)
				} else {
					msg.Nack(false, true)
				}
			}
			consumer.logger.Println("rabbit consumer goroutine closed")
		}()
	}
	consumer.logger.Printf("Processing messages on %v goroutines", consumeOptions.Concurrency)
	return nil
}
