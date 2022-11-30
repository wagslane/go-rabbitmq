package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// getDefaultConsumeOptions describes the options that will be used when a value isn't provided
func getDefaultConsumeOptions(queueName string) ConsumeOptions {
	return ConsumeOptions{
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
		ExchangeOptions: ExchangeOptions{
			Name:       "",
			Kind:       amqp.ExchangeDirect,
			Durable:    false,
			AutoDelete: false,
			Internal:   false,
			NoWait:     false,
			Passive:    false,
			Args:       Table{},
			Declare:    true,
		},
		Bindings:    []Binding{},
		Concurrency: 1,
	}
}

func getDefaultBindingOptions() BindingOptions {
	return BindingOptions{
		NoWait:  false,
		Args:    Table{},
		Declare: true,
	}
}

// ConsumeOptions are used to describe how a new consumer will be created.
// If QueueOptions is not nil, the options will be used to declare a queue
// If ExchangeOptions is not nil, it will be used to declare an exchange
// If there are Bindings, the queue will be bound to them
type ConsumeOptions struct {
	RabbitConsumerOptions RabbitConsumerOptions
	QueueOptions          QueueOptions
	ExchangeOptions       ExchangeOptions
	Bindings              []Binding
	Concurrency           int
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

// WithConsumeOptionsQueueDurable ensures the queue is a durable queue
func WithConsumeOptionsQueueDurable(options *ConsumeOptions) {
	options.QueueOptions.Durable = true
}

// WithConsumeOptionsQueueAutoDelete ensures the queue is an auto-delete queue
func WithConsumeOptionsQueueAutoDelete(options *ConsumeOptions) {
	options.QueueOptions.AutoDelete = true
}

// WithConsumeOptionsQueueExclusive ensures the queue is an exclusive queue
func WithConsumeOptionsQueueExclusive(options *ConsumeOptions) {
	options.QueueOptions.Exclusive = true
}

// WithConsumeOptionsQueueNoWait ensures the queue is a no-wait queue
func WithConsumeOptionsQueueNoWait(options *ConsumeOptions) {
	options.QueueOptions.NoWait = true
}

// WithConsumeOptionsQueuePassive ensures the queue is a passive queue
func WithConsumeOptionsQueuePassive(options *ConsumeOptions) {
	options.QueueOptions.Passive = true
}

// WithConsumeOptionsQueueNoDeclare will turn off the declaration of the queue's
// existance upon startup
func WithConsumeOptionsQueueNoDeclare(options *ConsumeOptions) {
	options.QueueOptions.Declare = false
}

// WithConsumeOptionsQueueArgs adds optional args to the queue
func WithConsumeOptionsQueueArgs(args Table) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		options.QueueOptions.Args = args
	}
}

// WithConsumeOptionsExchangeName sets the exchange name
func WithConsumeOptionsExchangeName(name string) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		options.ExchangeOptions.Name = name
	}
}

// WithConsumeOptionsExchangeKind ensures the queue is a durable queue
func WithConsumeOptionsExchangeKind(kind string) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		options.ExchangeOptions.Kind = kind
	}
}

// WithConsumeOptionsExchangeDurable ensures the exchange is a durable exchange
func WithConsumeOptionsExchangeDurable(options *ConsumeOptions) {
	options.ExchangeOptions.Durable = true
}

// WithConsumeOptionsExchangeAutoDelete ensures the exchange is an auto-delete exchange
func WithConsumeOptionsExchangeAutoDelete(options *ConsumeOptions) {
	options.ExchangeOptions.AutoDelete = true
}

// WithConsumeOptionsExchangeInternal ensures the exchange is an internal exchange
func WithConsumeOptionsExchangeInternal(options *ConsumeOptions) {
	options.ExchangeOptions.Internal = true
}

// WithConsumeOptionsExchangeNoWait ensures the exchange is a no-wait exchange
func WithConsumeOptionsExchangeNoWait(options *ConsumeOptions) {
	options.ExchangeOptions.NoWait = true
}

// WithConsumeOptionsExchangeNoDeclare stops this library from declaring the exchanges existance
func WithConsumeOptionsExchangeNoDeclare(options *ConsumeOptions) {
	options.ExchangeOptions.Declare = false
}

// WithConsumeOptionsExchangePassive ensures the exchange is a passive exchange
func WithConsumeOptionsExchangePassive(options *ConsumeOptions) {
	options.ExchangeOptions.Passive = true
}

// WithConsumeOptionsExchangeArgs adds optional args to the exchange
func WithConsumeOptionsExchangeArgs(args Table) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		options.ExchangeOptions.Args = args
	}
}

// WithConsumeOptionsDefaultBinding binds the queue to a routing key with the default binding options
func WithConsumeOptionsDefaultBinding(routingKey string) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		options.Bindings = append(options.Bindings, Binding{
			RoutingKey:     routingKey,
			BindingOptions: getDefaultBindingOptions(),
		})
	}
}

// WithConsumeOptionsBinding adds a new binding to the queue which allows you to set the binding options
// on a per-binding basis. Keep in mind that everything in the BindingOptions struct will default to
// the zero value. If you want to declare your bindings for example, be sure to set Declare=true
func WithConsumeOptionsBinding(binding Binding) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		options.Bindings = append(options.Bindings, binding)
	}
}

// WithConsumeOptionsConcurrency returns a function that sets the concurrency, which means that
// many goroutines will be spawned to run the provided handler on messages
func WithConsumeOptionsConcurrency(concurrency int) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		options.Concurrency = concurrency
	}
}

// WithConsumeOptionsConsumerName returns a function that sets the name on the server of this consumer
// if unset a random name will be given
func WithConsumeOptionsConsumerName(consumerName string) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		options.RabbitConsumerOptions.Name = consumerName
	}
}

// WithConsumeOptionsConsumerAutoAck returns a function that sets the auto acknowledge property on the server of this consumer
// if unset the default will be used (false)
func WithConsumeOptionsConsumerAutoAck(autoAck bool) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		options.RabbitConsumerOptions.AutoAck = autoAck
	}
}

// WithConsumeOptionsConsumerExclusive sets the consumer to exclusive, which means
// the server will ensure that this is the sole consumer
// from this queue. When exclusive is false, the server will fairly distribute
// deliveries across multiple consumers.
func WithConsumeOptionsConsumerExclusive(options *ConsumeOptions) {
	options.RabbitConsumerOptions.Exclusive = true
}

// WithConsumeOptionsConsumerNoWait sets the consumer to nowait, which means
// it does not wait for the server to confirm the request and
// immediately begin deliveries. If it is not possible to consume, a channel
// exception will be raised and the channel will be closed.
func WithConsumeOptionsConsumerNoWait(options *ConsumeOptions) {
	options.RabbitConsumerOptions.NoWait = true
}
