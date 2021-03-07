package rabbitmq

import (
	"log"
	"time"

	"github.com/streadway/amqp"
)

// Consumer allows you to create and connect to queues for data consumption.
type Consumer struct {
	chManager *channelManager
	logger    logger
}

// Delivery captures the fields for a previously delivered message resident in
// a queue to be delivered by the server to a consumer from Channel.Consume or
// Channel.Get.
type Delivery struct {
	amqp.Delivery
}

// ConsumeOptions are used to describe how a new consumer will be created.
type ConsumeOptions struct {
	QueueOptions    QueueOptions
	BindingOptions  BindingOptions
	QosOptions      QosOptions
	ConsumerOptions ConsumerOptions
	Logging         bool
}

// QueueOptions -
type QueueOptions struct {
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       Table
}

// BindingOptions -
type BindingOptions struct {
	Exchange string
	NoWait   bool
	Args     Table
}

// QosOptions -
type QosOptions struct {
	Concurrency int
	Prefetch    int
	Global      bool
}

// ConsumerOptions -
type ConsumerOptions struct {
	Name      string
	AutoAck   bool
	Exclusive bool
	NoWait    bool
	NoLocal   bool
	Args      Table
}

// GetConsumer returns a new Consumer connected to the given rabbitmq server
func GetConsumer(url string, logging bool) (Consumer, error) {
	chManager, err := newChannelManager(url, logging)
	if err != nil {
		return Consumer{}, err
	}
	consumer := Consumer{
		chManager: chManager,
		logger:    logger{logging: logging},
	}
	return consumer, nil
}

// getDefaultConsumeOptions descibes the options that will be used when a value isn't provided
func getDefaultConsumeOptions() ConsumeOptions {
	return ConsumeOptions{
		QueueOptions: QueueOptions{
			Durable:    false,
			AutoDelete: false,
			Exclusive:  false,
			NoWait:     false,
			Args:       nil,
		},
		BindingOptions: BindingOptions{
			Exchange: "",
			NoWait:   false,
			Args:     nil,
		},
		QosOptions: QosOptions{
			Concurrency: 1,
			Prefetch:    10,
			Global:      false,
		},
		ConsumerOptions: ConsumerOptions{
			Name:      "",
			AutoAck:   false,
			Exclusive: false,
			NoWait:    false,
			NoLocal:   false,
			Args:      nil,
		},
	}
}

// fillInConsumeDefaults -
func fillInConsumeDefaults(consumeOptions ConsumeOptions) ConsumeOptions {
	defaults := getDefaultConsumeOptions()
	if consumeOptions.QosOptions.Concurrency < 1 {
		consumeOptions.QosOptions.Concurrency = defaults.QosOptions.Concurrency
	}
	return consumeOptions
}

// StartConsumers starts n goroutines where n="ConsumeOptions.QosOptions.Concurrency".
// Each goroutine spawns a handler that consumes off of the qiven queue which binds to the routing key(s).
// The provided handler is called once for each message. If the provided queue doesn't exist, it
// will be created on the cluster
func (consumer Consumer) StartConsumers(
	handler func(d Delivery) bool,
	consumeOptions *ConsumeOptions,
	queue string,
	routingKeys ...string,
) error {
	defaults := getDefaultConsumeOptions()
	finalOptions := ConsumeOptions{}
	if consumeOptions == nil {
		finalOptions = defaults
	} else {
		finalOptions = fillInConsumeDefaults(*consumeOptions)
	}

	err := consumer.startGoroutines(
		handler,
		finalOptions,
		queue,
		routingKeys...,
	)
	if err != nil {
		return err
	}

	go func() {
		for err := range consumer.chManager.notifyCancelOrClose {
			consumer.logger.Printf("consume cancel/close handler triggered. err: %v", err)
			consumer.startGoroutinesWithRetries(
				handler,
				finalOptions,
				queue,
				routingKeys...,
			)
		}
	}()
	return nil
}

// startGoroutinesWithRetries attempts to start consuming on a channel
// with an exponential backoff
func (consumer Consumer) startGoroutinesWithRetries(
	handler func(d Delivery) bool,
	consumeOptions ConsumeOptions,
	queue string,
	routingKeys ...string,
) {
	backoffTime := time.Second
	for {
		consumer.logger.Printf("waiting %s seconds to attempt to start consumer goroutines", backoffTime)
		time.Sleep(backoffTime)
		backoffTime *= 2
		err := consumer.startGoroutines(
			handler,
			consumeOptions,
			queue,
			routingKeys...,
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
	consumeOptions ConsumeOptions,
	queue string,
	routingKeys ...string,
) error {
	consumer.chManager.channelMux.RLock()
	defer consumer.chManager.channelMux.RUnlock()

	_, err := consumer.chManager.channel.QueueDeclare(
		queue,
		consumeOptions.QueueOptions.Durable,
		consumeOptions.QueueOptions.AutoDelete,
		consumeOptions.QueueOptions.Exclusive,
		consumeOptions.QueueOptions.NoWait,
		tableToAMQPTable(consumeOptions.QueueOptions.Args),
	)
	if err != nil {
		return err
	}

	for _, routingKey := range routingKeys {
		err = consumer.chManager.channel.QueueBind(
			queue,
			routingKey,
			consumeOptions.BindingOptions.Exchange,
			consumeOptions.BindingOptions.NoWait,
			tableToAMQPTable(consumeOptions.BindingOptions.Args),
		)
		if err != nil {
			return err
		}
	}

	err = consumer.chManager.channel.Qos(
		consumeOptions.QosOptions.Prefetch,
		0,
		consumeOptions.QosOptions.Global,
	)
	if err != nil {
		return err
	}

	msgs, err := consumer.chManager.channel.Consume(
		queue,
		consumeOptions.ConsumerOptions.Name,
		consumeOptions.ConsumerOptions.AutoAck,
		consumeOptions.ConsumerOptions.Exclusive,
		consumeOptions.ConsumerOptions.NoLocal, // no-local is not supported by RabbitMQ
		consumeOptions.ConsumerOptions.NoWait,
		tableToAMQPTable(consumeOptions.ConsumerOptions.Args),
	)
	if err != nil {
		return err
	}

	for i := 0; i < consumeOptions.QosOptions.Concurrency; i++ {
		go func() {
			for msg := range msgs {
				if consumeOptions.ConsumerOptions.AutoAck {
					handler(Delivery{msg})
					continue
				}
				if handler(Delivery{msg}) {
					msg.Ack(false)
				} else {
					msg.Nack(false, true)
				}
			}
			log.Println("rabbit consumer goroutine closed")
		}()
	}
	log.Printf("Processing messages on %v goroutines", consumeOptions.QosOptions.Concurrency)
	return nil
}
