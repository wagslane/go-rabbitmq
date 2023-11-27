package rabbitmq

import (
	"github.com/wagslane/go-rabbitmq/internal/logger"
)

// getDefaultConsumerOptions describes the options that will be used when a value isn't provided
func getDefaultSuperConsumerOptions(queueName string) SuperConsumerOptions {
	return SuperConsumerOptions{
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
		ExchangeOptionsSlice: []SuperExchangeOptions{
			// Name:       "",
			// Kind:       amqp.ExchangeDirect,
			// Durable:    false,
			// AutoDelete: false,
			// Internal:   false,
			// NoWait:     false,
			// Passive:    false,
			// Args:       Table{},
			// Declare:    false,
			// Bindings:   []Binding{},
		},
		Concurrency: 1,
		Logger:      stdDebugLogger{},
		QOSPrefetch: 10,
		QOSGlobal:   false,
	}
}

type SuperConsumerOptions struct {
	RabbitConsumerOptions RabbitConsumerOptions
	QueueOptions          QueueOptions
	ExchangeOptionsSlice  []SuperExchangeOptions
	Concurrency           int
	Logger                logger.Logger
	QOSPrefetch           int
	QOSGlobal             bool
}

type SuperExchangeOptions struct {
	Name       string
	Kind       string // possible values: empty string for default exchange or direct, topic, fanout
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Passive    bool // if false, a missing exchange will be created on the server
	Args       Table
	Declare    bool
	Bindings   []Binding
}

// WithSuperConsumerOptionsQueueDurable ensures the queue is a durable queue
func WithSuperConsumerOptionsQueueDurable(options *SuperConsumerOptions) {
	options.QueueOptions.Durable = true
}

// WithSuperConsumerOptionsQueueAutoDelete ensures the queue is an auto-delete queue
func WithSuperConsumerOptionsQueueAutoDelete(options *SuperConsumerOptions) {
	options.QueueOptions.AutoDelete = true
}

// WithSuperConsumerOptionsQueueExclusive ensures the queue is an exclusive queue
func WithSuperConsumerOptionsQueueExclusive(options *SuperConsumerOptions) {
	options.QueueOptions.Exclusive = true
}

// WithSuperConsumerOptionsQueueNoWait ensures the queue is a no-wait queue
func WithSuperConsumerOptionsQueueNoWait(options *SuperConsumerOptions) {
	options.QueueOptions.NoWait = true
}

// WithSuperConsumerOptionsQueuePassive ensures the queue is a passive queue
func WithSuperConsumerOptionsQueuePassive(options *SuperConsumerOptions) {
	options.QueueOptions.Passive = true
}

// WithSuperConsumerOptionsQueueNoDeclare will turn off the declaration of the queue's
// existance upon startup
func WithSuperConsumerOptionsQueueNoDeclare(options *SuperConsumerOptions) {
	options.QueueOptions.Declare = false
}

// WithSuperConsumerOptionsQueueArgs adds optional args to the queue
func WithSuperConsumerOptionsQueueArgs(args Table) func(*SuperConsumerOptions) {
	return func(options *SuperConsumerOptions) {
		options.QueueOptions.Args = args
	}
}

// WithSuperConsumerOptionsExchangeName sets the exchange name
func WithSuperConsumerOptionsExchangeName(name string) func(*SuperConsumerOptions) {
	return func(options *SuperConsumerOptions) {
		isExist := false
		for _, eo := range options.ExchangeOptionsSlice {
			if eo.Name == name {
				isExist = true
			}
		}
		if !isExist {
			options.ExchangeOptionsSlice = append(options.ExchangeOptionsSlice, SuperExchangeOptions{
				Name: name,
			})
		}
	}
}

// WithSuperConsumerOptionsExchangeKind ensures the queue is a durable queue
func WithSuperConsumerOptionsExchangeKind(kind string, name string) func(*SuperConsumerOptions) {
	return func(options *SuperConsumerOptions) {
		for _, eo := range options.ExchangeOptionsSlice {
			if eo.Name == name {
				eo.Kind = kind
			}
		}
	}
}

// WithSuperConsumerOptionsExchangeDurable ensures the exchange is a durable exchange
func WithSuperConsumerOptionsExchangeDurable(name string) func(*SuperConsumerOptions) {
	return func(options *SuperConsumerOptions) {
		for _, eo := range options.ExchangeOptionsSlice {
			if eo.Name == name {
				eo.Durable = true
			}
		}
	}
}

// WithSuperConsumerOptionsExchangeAutoDelete ensures the exchange is an auto-delete exchange
func WithSuperConsumerOptionsExchangeAutoDelete(name string) func(*SuperConsumerOptions) {
	return func(options *SuperConsumerOptions) {
		for _, eo := range options.ExchangeOptionsSlice {
			if eo.Name == name {
				eo.AutoDelete = true
			}
		}
	}
}

// WithSuperConsumerOptionsExchangeInternal ensures the exchange is an internal exchange
func WithSuperConsumerOptionsExchangeInternal(name string) func(*SuperConsumerOptions) {
	return func(options *SuperConsumerOptions) {
		for _, eo := range options.ExchangeOptionsSlice {
			if eo.Name == name {
				eo.Internal = true
			}
		}
	}
}

// WithSuperConsumerOptionsExchangeNoWait ensures the exchange is a no-wait exchange
func WithSuperConsumerOptionsExchangeNoWait(name string) func(*SuperConsumerOptions) {
	return func(options *SuperConsumerOptions) {
		for _, eo := range options.ExchangeOptionsSlice {
			if eo.Name == name {
				eo.NoWait = true
			}
		}
	}
}

// WithSuperConsumerOptionsExchangeDeclare stops this library from declaring the exchanges existance
func WithSuperConsumerOptionsExchangeDeclare(name string) func(*SuperConsumerOptions) {
	return func(options *SuperConsumerOptions) {
		for _, eo := range options.ExchangeOptionsSlice {
			if eo.Name == name {
				eo.Declare = true
			}
		}
	}
}

// WithSuperConsumerOptionsExchangePassive ensures the exchange is a passive exchange
func WithSuperConsumerOptionsExchangePassive(name string) func(*SuperConsumerOptions) {
	return func(options *SuperConsumerOptions) {
		for _, eo := range options.ExchangeOptionsSlice {
			if eo.Name == name {
				eo.Passive = true
			}
		}
	}
}

// WithSuperConsumerOptionsExchangeArgs adds optional args to the exchange
func WithSuperConsumerOptionsExchangeArgs(args Table, name string) func(*SuperConsumerOptions) {
	return func(options *SuperConsumerOptions) {
		for _, eo := range options.ExchangeOptionsSlice {
			if eo.Name == name {
				eo.Args = args
			}
		}
	}
}

// WithSuperConsumerOptionsRoutingKey binds the queue to a routing key with the default binding options
func WithSuperConsumerOptionsRoutingKey(routingKey string, name string) func(*SuperConsumerOptions) {
	return func(options *SuperConsumerOptions) {
		for _, eo := range options.ExchangeOptionsSlice {
			if eo.Name == name {
				eo.Bindings = append(eo.Bindings, Binding{
					RoutingKey:     routingKey,
					BindingOptions: getDefaultBindingOptions(),
				})
			}
		}
	}
}

// WithSuperConsumerOptionsBinding adds a new binding to the queue which allows you to set the binding options
// on a per-binding basis. Keep in mind that everything in the BindingOptions struct will default to
// the zero value. If you want to declare your bindings for example, be sure to set Declare=true
func WithSuperConsumerOptionsBinding(binding Binding, name string) func(*SuperConsumerOptions) {
	return func(options *SuperConsumerOptions) {
		for _, eo := range options.ExchangeOptionsSlice {
			if eo.Name == name {
				eo.Bindings = append(eo.Bindings, binding)
			}
		}
	}
}

// WithSuperConsumerOptionsConcurrency returns a function that sets the concurrency, which means that
// many goroutines will be spawned to run the provided handler on messages
func WithSuperConsumerOptionsConcurrency(concurrency int) func(*SuperConsumerOptions) {
	return func(options *SuperConsumerOptions) {
		options.Concurrency = concurrency
	}
}

// WithSuperConsumerOptionsConsumerName returns a function that sets the name on the server of this consumer
// if unset a random name will be given
func WithSuperConsumerOptionsConsumerName(consumerName string) func(*SuperConsumerOptions) {
	return func(options *SuperConsumerOptions) {
		options.RabbitConsumerOptions.Name = consumerName
	}
}

// WithSuperConsumerOptionsConsumerAutoAck returns a function that sets the auto acknowledge property on the server of this consumer
// if unset the default will be used (false)
func WithSuperConsumerOptionsConsumerAutoAck(autoAck bool) func(*SuperConsumerOptions) {
	return func(options *SuperConsumerOptions) {
		options.RabbitConsumerOptions.AutoAck = autoAck
	}
}

// WithSuperConsumerOptionsConsumerExclusive sets the consumer to exclusive, which means
// the server will ensure that this is the sole consumer
// from this queue. When exclusive is false, the server will fairly distribute
// deliveries across multiple consumers.
func WithSuperConsumerOptionsConsumerExclusive(options *SuperConsumerOptions) {
	options.RabbitConsumerOptions.Exclusive = true
}

// WithSuperConsumerOptionsConsumerNoWait sets the consumer to nowait, which means
// it does not wait for the server to confirm the request and
// immediately begin deliveries. If it is not possible to consume, a channel
// exception will be raised and the channel will be closed.
func WithSuperConsumerOptionsConsumerNoWait(options *SuperConsumerOptions) {
	options.RabbitConsumerOptions.NoWait = true
}

// WithSuperConsumerOptionsLogging uses a default logger that writes to std out
func WithSuperConsumerOptionsLogging(options *SuperConsumerOptions) {
	options.Logger = &stdDebugLogger{}
}

// WithSuperConsumerOptionsLogger sets logging to a custom interface.
// Use WithSuperConsumerOptionsLogging to just log to stdout.
func WithSuperConsumerOptionsLogger(log logger.Logger) func(options *SuperConsumerOptions) {
	return func(options *SuperConsumerOptions) {
		options.Logger = log
	}
}

// WithSuperConsumerOptionsQOSPrefetch returns a function that sets the prefetch count, which means that
// many messages will be fetched from the server in advance to help with throughput.
// This doesn't affect the handler, messages are still processed one at a time.
func WithSuperConsumerOptionsQOSPrefetch(prefetchCount int) func(*SuperConsumerOptions) {
	return func(options *SuperConsumerOptions) {
		options.QOSPrefetch = prefetchCount
	}
}

// WithSuperConsumerOptionsQOSGlobal sets the qos on the channel to global, which means
// these QOS settings apply to ALL existing and future
// consumers on all channels on the same connection
func WithSuperConsumerOptionsQOSGlobal(options *SuperConsumerOptions) {
	options.QOSGlobal = true
}

// WithSuperConsumerOptionsQueueQuorum sets the queue a quorum type, which means
// multiple nodes in the cluster will have the messages distributed amongst them
// for higher reliability
func WithSuperConsumerOptionsQueueQuorum(options *SuperConsumerOptions) {
	if options.QueueOptions.Args == nil {
		options.QueueOptions.Args = Table{}
	}

	options.QueueOptions.Args["x-queue-type"] = "quorum"
}
