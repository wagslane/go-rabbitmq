package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/wagslane/go-rabbitmq/internal/logger"
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
		Queues: []QueueOptions{
			{
				Name:       queueName,
				Durable:    false,
				AutoDelete: false,
				Exclusive:  false,
				NoWait:     false,
				Passive:    false,
				Args:       Table{},
				Declare:    true,
			},
		},
		Exchanges: []ExchangeOptions{
			{
				Name:       "",
				Kind:       amqp.ExchangeDirect,
				Durable:    false,
				AutoDelete: false,
				Internal:   false,
				NoWait:     false,
				Passive:    false,
				Args:       Table{},
				Declare:    false,
			},
		},
		Bindings:    []Binding{},
		Concurrency: 1,
		Logger:      stdDebugLogger{},
		QOSPrefetch: 10,
		QOSGlobal:   false,
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
	QueueName             string
	Queues                []QueueOptions
	Exchanges             []ExchangeOptions
	Bindings              []Binding
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

// WithConsumerOptionsQueueDurable ensures the queue is a durable queue
func WithConsumerOptionsQueueDurable(options *ConsumerOptions) {
	WithSimpleQueueOptions(options, func(queueOptions *QueueOptions) {
		queueOptions.Durable = true
	})
}

// WithConsumerOptionsQueueAutoDelete ensures the queue is an auto-delete queue
func WithConsumerOptionsQueueAutoDelete(options *ConsumerOptions) {
	WithSimpleQueueOptions(options, func(queueOptions *QueueOptions) {
		queueOptions.AutoDelete = true
	})
}

// WithConsumerOptionsQueueExclusive ensures the queue is an exclusive queue
func WithConsumerOptionsQueueExclusive(options *ConsumerOptions) {
	WithSimpleQueueOptions(options, func(queueOptions *QueueOptions) {
		queueOptions.Exclusive = true
	})
}

// WithConsumerOptionsQueueNoWait ensures the queue is a no-wait queue
func WithConsumerOptionsQueueNoWait(options *ConsumerOptions) {
	WithSimpleQueueOptions(options, func(queueOptions *QueueOptions) {
		queueOptions.NoWait = true
	})
}

// WithConsumerOptionsQueuePassive ensures the queue is a passive queue
func WithConsumerOptionsQueuePassive(options *ConsumerOptions) {
	WithSimpleQueueOptions(options, func(queueOptions *QueueOptions) {
		queueOptions.Passive = true
	})
}

// WithConsumerOptionsQueueNoDeclare will turn off the declaration of the queue's
// existance upon startup
func WithConsumerOptionsQueueNoDeclare(options *ConsumerOptions) {
	WithSimpleQueueOptions(options, func(queueOptions *QueueOptions) {
		queueOptions.Declare = false
	})
}

// WithConsumerOptionsQueueArgs adds optional args to the queue
func WithConsumerOptionsQueueArgs(args Table) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		WithSimpleQueueOptions(options, func(queueOptions *QueueOptions) {
			queueOptions.Args = args
		})
	}
}

// WithConsumerOptionsExchangeName sets the exchange name
func WithConsumerOptionsExchangeName(name string) func(*ConsumerOptions) {

	return func(options *ConsumerOptions) {
		WithSimpleExchangeOptions(options, func(exchangeOptions *ExchangeOptions) {
			exchangeOptions.Name = name
		})
	}
}

// WithConsumerOptionsExchangeKind ensures the queue is a durable queue
func WithConsumerOptionsExchangeKind(kind string) func(*ConsumerOptions) {

	return func(options *ConsumerOptions) {
		WithSimpleExchangeOptions(options, func(exchangeOptions *ExchangeOptions) {
			exchangeOptions.Kind = kind
		})
	}
}

// WithConsumerOptionsExchangeDurable ensures the exchange is a durable exchange
func WithConsumerOptionsExchangeDurable(options *ConsumerOptions) {
	WithSimpleExchangeOptions(options, func(exchangeOptions *ExchangeOptions) {
		exchangeOptions.Durable = true
	})
}

// WithConsumerOptionsExchangeAutoDelete ensures the exchange is an auto-delete exchange
func WithConsumerOptionsExchangeAutoDelete(options *ConsumerOptions) {
	WithSimpleExchangeOptions(options, func(exchangeOptions *ExchangeOptions) {
		exchangeOptions.AutoDelete = true
	})
}

// WithConsumerOptionsExchangeInternal ensures the exchange is an internal exchange
func WithConsumerOptionsExchangeInternal(options *ConsumerOptions) {
	WithSimpleExchangeOptions(options, func(exchangeOptions *ExchangeOptions) {
		exchangeOptions.Internal = true
	})
}

// WithConsumerOptionsExchangeNoWait ensures the exchange is a no-wait exchange
func WithConsumerOptionsExchangeNoWait(options *ConsumerOptions) {
	WithSimpleExchangeOptions(options, func(exchangeOptions *ExchangeOptions) {
		exchangeOptions.NoWait = true
	})
}

// WithConsumerOptionsExchangeDeclare stops this library from declaring the exchanges existance
func WithConsumerOptionsExchangeDeclare(options *ConsumerOptions) {
	WithSimpleExchangeOptions(options, func(exchangeOptions *ExchangeOptions) {
		exchangeOptions.Declare = true
	})
}

// WithConsumerOptionsExchangePassive ensures the exchange is a passive exchange
func WithConsumerOptionsExchangePassive(options *ConsumerOptions) {
	WithSimpleExchangeOptions(options, func(exchangeOptions *ExchangeOptions) {
		exchangeOptions.Passive = true
	})
}

// WithConsumerOptionsExchangeArgs adds optional args to the exchange
func WithConsumerOptionsExchangeArgs(args Table) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		WithSimpleExchangeOptions(options, func(exchangeOptions *ExchangeOptions) {
			exchangeOptions.Args = args
		})
	}
}

// WithConsumerOptionsRoutingKey binds the queue to a routing key with the default binding options
func WithConsumerOptionsRoutingKey(routingKey string) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.Bindings = append(options.Bindings, Binding{
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
		options.Bindings = append(options.Bindings, binding)
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

// WithConsumerOptionsQueueQuorum sets the queue a quorum type, which means
// multiple nodes in the cluster will have the messages distributed amongst them
// for higher reliability
func WithConsumerOptionsQueueQuorum(options *ConsumerOptions) {
	WithSimpleQueueOptions(options, func(queueOptions *QueueOptions) {
		if queueOptions.Args == nil {
			queueOptions.Args = Table{}
		}

		queueOptions.Args["x-queue-type"] = "quorum"
	})
}

// WithSimpleQueueOptions used for backwards compatibility
// Will set options on the first queue and ensure that queue exists
func WithSimpleQueueOptions(options *ConsumerOptions, handler func(queueOptions *QueueOptions)) {
	if len(options.Queues) == 0 {
		options.Queues = append(options.Queues, QueueOptions{})
	}

	handler(&options.Queues[0])
}

// WithSimpleExchangeOptions used for backwards compatibility
// Will set options on the first exchange and ensure that exchange exists
func WithSimpleExchangeOptions(options *ConsumerOptions, handler func(exchangeOptions *ExchangeOptions)) {
	if len(options.Exchanges) == 0 {
		options.Exchanges = append(options.Exchanges, ExchangeOptions{})
	}

	handler(&options.Exchanges[0])
}

func WithConsumerQueue(queue QueueOptions) func(options *ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.Queues = []QueueOptions{queue}
	}
}

func WithConsumerQueues(queues []QueueOptions) func(options *ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.Queues = queues
	}
}

func WithConsumerBindings(bindings []Binding) func(options *ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.Bindings = bindings
	}
}

func WithConsumerExchanges(exchanges []ExchangeOptions) func(options *ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.Exchanges = exchanges
	}
}
