package rabbitmq

import amqp "github.com/rabbitmq/amqp091-go"

// PublisherOptions are used to describe a publisher's configuration.
// Logger is a custom logging interface.
type PublisherOptions struct {
	ExchangeOptions ExchangeOptions
	ConfirmMode     bool
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
		ConfirmMode: false,
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

// WithPublisherOptionsExchangeDeclare will create the exchange if it doesn't exist
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

// WithPublisherOptionsConfirm enables confirm mode on the connection
// this is required if publisher confirmations should be used
func WithPublisherOptionsConfirm(options *PublisherOptions) {
	options.ConfirmMode = true
}
