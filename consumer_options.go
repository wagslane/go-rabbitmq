package rabbitmq

import (
	"github.com/makometr/go-rabbitmq/internal/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

// getDefaultConsumerOptions describes the options that will be used when a value isn't provided
func getDefaultConsumerOptions(queueName string) ConsumerOptions {
	return ConsumerOptions{
		RabbitConsumerOptions: RabbitConsumerOptions{
			Name:      "",
			AutoAck:   false,
			Exclusive: false,
			NoWait:    false,
			NoLocal:   false,
			Args:      Table{},
		},
		QueueOptions: QueueOptions{
			Name:       queueName,
			Durable:    false,
			AutoDelete: false,
			Exclusive:  false,
			NoWait:     false,
			Passive:    false,
			Args:       Table{},
			Declare:    true,
		},
		ExchangeOptions: []ExchangeOptions{},
		Concurrency:     1,
		CloseGracefully: true,
		Logger:          stdDebugLogger{},
		QOSPrefetch:     10,
		QOSGlobal:       false,
	}
}

func getDefaultExchangeOptions() ExchangeOptions {
	return ExchangeOptions{
		Name:       "",
		Kind:       amqp.ExchangeDirect,
		Durable:    false,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Passive:    false,
		Args:       Table{},
		Declare:    false,
		Bindings:   []Binding{},
	}
}

func getDefaultBindingOptions() BindingOptions {
	return BindingOptions{
		NoWait:  false,
		Args:    Table{},
		Declare: true,
	}
}

// ConsumerOptions are used to describe how a new consumer will be created.
// If QueueOptions is not nil, the options will be used to declare a queue
// If ExchangeOptions is not nil, it will be used to declare an exchange
// If there are Bindings, the queue will be bound to them
type ConsumerOptions struct {
	RabbitConsumerOptions RabbitConsumerOptions
	QueueOptions          QueueOptions
	CloseGracefully       bool
	ExchangeOptions       []ExchangeOptions
	Concurrency           int
	Logger                logger.Logger
	QOSPrefetch           int
	QOSGlobal             bool
}

// RabbitConsumerOptions are used to configure the consumer
// on the rabbit server
type RabbitConsumerOptions struct {
	Name      string
	AutoAck   bool
	Exclusive bool
	NoWait    bool
	NoLocal   bool
	Args      Table
}

// QueueOptions are used to configure a queue.
// A passive queue is assumed by RabbitMQ to already exist, and attempting to connect
// to a non-existent queue will cause RabbitMQ to throw an exception.
type QueueOptions struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Passive    bool // if false, a missing queue will be created on the server
	Args       Table
	Declare    bool
}

// Binding describes the bhinding of a queue to a routing key on an exchange
type Binding struct {
	RoutingKey string
	BindingOptions
}

// BindingOptions describes the options a binding can have
type BindingOptions struct {
	NoWait  bool
	Args    Table
	Declare bool
}

// WithConsumerOptionsQueueDurable ensures the queue is a durable queue
func WithConsumerOptionsQueueDurable(options *ConsumerOptions) {
	options.QueueOptions.Durable = true
}

// WithConsumerOptionsQueueAutoDelete ensures the queue is an auto-delete queue
func WithConsumerOptionsQueueAutoDelete(options *ConsumerOptions) {
	options.QueueOptions.AutoDelete = true
}

// WithConsumerOptionsQueueExclusive ensures the queue is an exclusive queue
func WithConsumerOptionsQueueExclusive(options *ConsumerOptions) {
	options.QueueOptions.Exclusive = true
}

// WithConsumerOptionsQueueNoWait ensures the queue is a no-wait queue
func WithConsumerOptionsQueueNoWait(options *ConsumerOptions) {
	options.QueueOptions.NoWait = true
}

// WithConsumerOptionsQueuePassive ensures the queue is a passive queue
func WithConsumerOptionsQueuePassive(options *ConsumerOptions) {
	options.QueueOptions.Passive = true
}

// WithConsumerOptionsQueueNoDeclare will turn off the declaration of the queue's
// existance upon startup
func WithConsumerOptionsQueueNoDeclare(options *ConsumerOptions) {
	options.QueueOptions.Declare = false
}

// WithConsumerOptionsQueueArgs adds optional args to the queue
func WithConsumerOptionsQueueArgs(args Table) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.QueueOptions.Args = args
	}
}

func ensureExchangeOptions(options *ConsumerOptions) {
	if len(options.ExchangeOptions) == 0 {
		options.ExchangeOptions = append(options.ExchangeOptions, getDefaultExchangeOptions())
	}
}

// WithConsumerOptionsExchangeName sets the exchange name
func WithConsumerOptionsExchangeName(name string) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		ensureExchangeOptions(options)
		options.ExchangeOptions[0].Name = name
	}
}

// WithConsumerOptionsExchangeKind ensures the queue is a durable queue
func WithConsumerOptionsExchangeKind(kind string) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		ensureExchangeOptions(options)
		options.ExchangeOptions[0].Kind = kind
	}
}

// WithConsumerOptionsExchangeDurable ensures the exchange is a durable exchange
func WithConsumerOptionsExchangeDurable(options *ConsumerOptions) {
	ensureExchangeOptions(options)
	options.ExchangeOptions[0].Durable = true
}

// WithConsumerOptionsExchangeAutoDelete ensures the exchange is an auto-delete exchange
func WithConsumerOptionsExchangeAutoDelete(options *ConsumerOptions) {
	ensureExchangeOptions(options)
	options.ExchangeOptions[0].AutoDelete = true
}

// WithConsumerOptionsExchangeInternal ensures the exchange is an internal exchange
func WithConsumerOptionsExchangeInternal(options *ConsumerOptions) {
	ensureExchangeOptions(options)
	options.ExchangeOptions[0].Internal = true
}

// WithConsumerOptionsExchangeNoWait ensures the exchange is a no-wait exchange
func WithConsumerOptionsExchangeNoWait(options *ConsumerOptions) {
	ensureExchangeOptions(options)
	options.ExchangeOptions[0].NoWait = true
}

// WithConsumerOptionsExchangeDeclare stops this library from declaring the exchanges existance
func WithConsumerOptionsExchangeDeclare(options *ConsumerOptions) {
	ensureExchangeOptions(options)
	options.ExchangeOptions[0].Declare = true
}

// WithConsumerOptionsExchangePassive ensures the exchange is a passive exchange
func WithConsumerOptionsExchangePassive(options *ConsumerOptions) {
	ensureExchangeOptions(options)
	options.ExchangeOptions[0].Passive = true
}

// WithConsumerOptionsExchangeArgs adds optional args to the exchange
func WithConsumerOptionsExchangeArgs(args Table) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		ensureExchangeOptions(options)
		options.ExchangeOptions[0].Args = args
	}
}

// WithConsumerOptionsRoutingKey binds the queue to a routing key with the default binding options
func WithConsumerOptionsRoutingKey(routingKey string) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		ensureExchangeOptions(options)
		options.ExchangeOptions[0].Bindings = append(options.ExchangeOptions[0].Bindings, Binding{
			RoutingKey:     routingKey,
			BindingOptions: getDefaultBindingOptions(),
		})
	}
}

// WithConsumerOptionsBinding adds a new binding to the queue which allows you to set the binding options
// on a per-binding basis. Keep in mind that everything in the BindingOptions struct will default to
// the zero value. If you want to declare your bindings for example, be sure to set Declare=true
func WithConsumerOptionsBinding(binding Binding) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		ensureExchangeOptions(options)
		options.ExchangeOptions[0].Bindings = append(options.ExchangeOptions[0].Bindings, binding)
	}
}

// WithConsumerOptionsExchangeOptions adds a new exchange to the consumer, this should probably only be
// used if you want to to consume from multiple exchanges on the same consumer
func WithConsumerOptionsExchangeOptions(exchangeOptions ExchangeOptions) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.ExchangeOptions = append(options.ExchangeOptions, exchangeOptions)
	}
}

// WithConsumerOptionsConcurrency returns a function that sets the concurrency, which means that
// many goroutines will be spawned to run the provided handler on messages
func WithConsumerOptionsConcurrency(concurrency int) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.Concurrency = concurrency
	}
}

// WithConsumerOptionsConsumerName returns a function that sets the name on the server of this consumer
// if unset a random name will be given
func WithConsumerOptionsConsumerName(consumerName string) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.RabbitConsumerOptions.Name = consumerName
	}
}

// WithConsumerOptionsConsumerAutoAck returns a function that sets the auto acknowledge property on the server of this consumer
// if unset the default will be used (false)
func WithConsumerOptionsConsumerAutoAck(autoAck bool) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.RabbitConsumerOptions.AutoAck = autoAck
	}
}

// WithConsumerOptionsConsumerExclusive sets the consumer to exclusive, which means
// the server will ensure that this is the sole consumer
// from this queue. When exclusive is false, the server will fairly distribute
// deliveries across multiple consumers.
func WithConsumerOptionsConsumerExclusive(options *ConsumerOptions) {
	options.RabbitConsumerOptions.Exclusive = true
}

// WithConsumerOptionsConsumerNoWait sets the consumer to nowait, which means
// it does not wait for the server to confirm the request and
// immediately begin deliveries. If it is not possible to consume, a channel
// exception will be raised and the channel will be closed.
func WithConsumerOptionsConsumerNoWait(options *ConsumerOptions) {
	options.RabbitConsumerOptions.NoWait = true
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

// WithConsumerOptionsQOSPrefetch returns a function that sets the prefetch count, which means that
// many messages will be fetched from the server in advance to help with throughput.
// This doesn't affect the handler, messages are still processed one at a time.
func WithConsumerOptionsQOSPrefetch(prefetchCount int) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.QOSPrefetch = prefetchCount
	}
}

// WithConsumerOptionsQOSGlobal sets the qos on the channel to global, which means
// these QOS settings apply to ALL existing and future
// consumers on all channels on the same connection
func WithConsumerOptionsQOSGlobal(options *ConsumerOptions) {
	options.QOSGlobal = true
}

// WithConsumerOptionsForceShutdown tells the consumer to not wait for
// the handler to complete in consumer.Close
func WithConsumerOptionsForceShutdown(options *ConsumerOptions) {
	options.CloseGracefully = false
}

// WithConsumerOptionsQueueQuorum sets the queue a quorum type, which means
// multiple nodes in the cluster will have the messages distributed amongst them
// for higher reliability
func WithConsumerOptionsQueueQuorum(options *ConsumerOptions) {
	if options.QueueOptions.Args == nil {
		options.QueueOptions.Args = Table{}
	}

	options.QueueOptions.Args["x-queue-type"] = "quorum"
}
