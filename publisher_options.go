package rabbitmq

import amqp "github.com/rabbitmq/amqp091-go"

// PublisherOptions are used to describe a publisher's configuration.
// Logger is a custom logging interface.
type PublisherOptions struct {
	ExchangeOptions ExchangeOptions
	Logger          Logger
}

// getDefaultPublisherOptions describes the options that will be used when a value isn't provided
func getDefaultPublisherOptions() PublisherOptions {
	return PublisherOptions{
		ExchangeOptions: ExchangeOptions{
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
		Logger: stdDebugLogger{},
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
		options.ExchangeOptions.Name = name
	}
}

// WithPublisherOptionsExchangeKind ensures the queue is a durable queue
func WithPublisherOptionsExchangeKind(kind string) func(*PublisherOptions) {
	return func(options *PublisherOptions) {
		options.ExchangeOptions.Kind = kind
	}
}

// WithPublisherOptionsExchangeDurable ensures the exchange is a durable exchange
func WithPublisherOptionsExchangeDurable(options *PublisherOptions) {
	options.ExchangeOptions.Durable = true
}

// WithPublisherOptionsExchangeAutoDelete ensures the exchange is an auto-delete exchange
func WithPublisherOptionsExchangeAutoDelete(options *PublisherOptions) {
	options.ExchangeOptions.AutoDelete = true
}

// WithPublisherOptionsExchangeInternal ensures the exchange is an internal exchange
func WithPublisherOptionsExchangeInternal(options *PublisherOptions) {
	options.ExchangeOptions.Internal = true
}

// WithPublisherOptionsExchangeNoWait ensures the exchange is a no-wait exchange
func WithPublisherOptionsExchangeNoWait(options *PublisherOptions) {
	options.ExchangeOptions.NoWait = true
}

// WithPublisherOptionsExchangeDeclare stops this library from declaring the exchanges existance
func WithPublisherOptionsExchangeDeclare(options *PublisherOptions) {
	options.ExchangeOptions.Declare = true
}

// WithPublisherOptionsExchangePassive ensures the exchange is a passive exchange
func WithPublisherOptionsExchangePassive(options *PublisherOptions) {
	options.ExchangeOptions.Passive = true
}

// WithPublisherOptionsExchangeArgs adds optional args to the exchange
func WithPublisherOptionsExchangeArgs(args Table) func(*PublisherOptions) {
	return func(options *PublisherOptions) {
		options.ExchangeOptions.Args = args
	}
}
