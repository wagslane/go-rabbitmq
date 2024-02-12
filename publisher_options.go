package rabbitmq

import amqp "github.com/rabbitmq/amqp091-go"

// PublisherOptions are used to describe a publisher's configuration.
// Logger is a custom logging interface.
type PublisherOptions struct {
	ExchangeName string
	Logger       Logger
	ConfirmMode  bool

	// Declare these queues, exchanges, and bindings before publishing
	Queues    []QueueOptions
	Exchanges []ExchangeOptions
	Bindings  []Binding
}

// getDefaultPublisherOptions describes the options that will be used when a value isn't provided
func getDefaultPublisherOptions() PublisherOptions {
	return PublisherOptions{
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
		Logger:      stdDebugLogger{},
		ConfirmMode: false,
	}
}

// WithPublisherOptionsLogging sets logging to true on the publisher options
// and sets the
func WithPublisherOptionsLogging(options *PublisherOptions) {
	options.Logger = &stdDebugLogger{}
}

// WithPublisherOptionsLogger sets logging to a custom interface.
// Use WithPublisherOptionsLogging to just log to stdout.
func WithPublisherOptionsLogger(log Logger) func(options *PublisherOptions) {
	return func(options *PublisherOptions) {
		options.Logger = log
	}
}

// WithPublisherOptionsExchangeName sets the exchange name
func WithPublisherOptionsExchangeName(name string) func(*PublisherOptions) {
	return func(options *PublisherOptions) {
		if options.Exchanges[0].Name == "" {
			options.Exchanges[0].Name = name
		}
		options.ExchangeName = name
	}
}
func WithPublisherOptionsConfirmMode(confirm bool) func(*PublisherOptions) {
	return func(options *PublisherOptions) {
		options.ConfirmMode = confirm
	}
}

// WithPublisherOptionsExchangeKind ensures the queue is a durable queue
func WithPublisherOptionsExchangeKind(kind string) func(*PublisherOptions) {
	return func(options *PublisherOptions) {
		options.Exchanges[0].Kind = kind
	}
}

// WithPublisherOptionsExchangeDurable ensures the exchange is a durable exchange
func WithPublisherOptionsExchangeDurable(options *PublisherOptions) {
	options.Exchanges[0].Durable = true
}

// WithPublisherOptionsExchangeAutoDelete ensures the exchange is an auto-delete exchange
func WithPublisherOptionsExchangeAutoDelete(options *PublisherOptions) {
	options.Exchanges[0].AutoDelete = true
}

// WithPublisherOptionsExchangeInternal ensures the exchange is an internal exchange
func WithPublisherOptionsExchangeInternal(options *PublisherOptions) {
	options.Exchanges[0].Internal = true
}

// WithPublisherOptionsExchangeNoWait ensures the exchange is a no-wait exchange
func WithPublisherOptionsExchangeNoWait(options *PublisherOptions) {
	options.Exchanges[0].NoWait = true
}

// WithPublisherOptionsExchangeDeclare stops this library from declaring the exchanges existance
func WithPublisherOptionsExchangeDeclare(options *PublisherOptions) {
	options.Exchanges[0].Declare = true
}

// WithPublisherOptionsExchangePassive ensures the exchange is a passive exchange
func WithPublisherOptionsExchangePassive(options *PublisherOptions) {
	options.Exchanges[0].Passive = true
}

// WithPublisherOptionsExchangeArgs adds optional args to the exchange
func WithPublisherOptionsExchangeArgs(args Table) func(*PublisherOptions) {
	return func(options *PublisherOptions) {
		options.Exchanges[0].Args = args
	}
}

// WithPublisherOptionsConfirm enables confirm mode on the connection
// this is required if publisher confirmations should be used
func WithPublisherOptionsConfirm(options *PublisherOptions) {
	options.ConfirmMode = true
}

func WithPublisherQueues(queues []QueueOptions) func(options *PublisherOptions) {
	return func(options *PublisherOptions) {
		options.Queues = queues
	}
}

func WithPublisherBindings(bindings []Binding) func(options *PublisherOptions) {
	return func(options *PublisherOptions) {
		options.Bindings = bindings
	}
}

func WithPublisherExchange(exchange ExchangeOptions) func(options *PublisherOptions) {
	return func(options *PublisherOptions) {
		options.Exchanges = []ExchangeOptions{exchange}
	}
}

func WithPublisherExchanges(exchanges []ExchangeOptions) func(options *PublisherOptions) {
	return func(options *PublisherOptions) {
		options.Exchanges = exchanges
	}
}
