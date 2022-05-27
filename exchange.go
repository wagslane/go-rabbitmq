package rabbitmq

import amqp "github.com/rabbitmq/amqp091-go"

// ExchangeOptions are used when configuring or binding to an exchange.
// it will verify the exchange is created before binding to it.
type ExchangeOptions struct {
	Name         string
	Kind         string
	Durable      bool
	AutoDelete   bool
	Internal     bool
	NoWait       bool
	ExchangeArgs Table
	Declare      bool
}

// getConsumerExchangeOptionsOrSetDefault returns pointer to current Exchange options. if no Exchange options are set yet, it will set it with default values.
func getConsumerExchangeOptionsOrSetDefault(options *ConsumeOptions) *ExchangeOptions {
	if options.ExchangeOptions == nil {
		options.ExchangeOptions = getDefaultExchangeOptions()
	}
	return options.ExchangeOptions
}

// getPublisherExchangeOptionsOrSetDefault returns pointer to current Exchange options. if no Exchange options are set yet, it will set it with default values.
func getPublisherExchangeOptionsOrSetDefault(options *PublisherOptions) *ExchangeOptions {
	if options.ExchangeOptions == nil {
		options.ExchangeOptions = getDefaultExchangeOptions()
	}
	return options.ExchangeOptions
}

// getDefaultExchangeOptions returns pointer to the default Exchange options.
func getDefaultExchangeOptions() *ExchangeOptions {
	return &ExchangeOptions{
		Name:         "",
		Kind:         "direct",
		Durable:      false,
		AutoDelete:   false,
		Internal:     false,
		NoWait:       false,
		ExchangeArgs: nil,
		Declare:      true,
	}
}

// getDefaultExchangeOptions declares or verifies the existence of an exchange.
func declareOrVerifyExchange(exchangeOptions *ExchangeOptions, channel *amqp.Channel) error {
	if exchangeOptions.Declare {
		return channel.ExchangeDeclare(
			exchangeOptions.Name,
			exchangeOptions.Kind,
			exchangeOptions.Durable,
			exchangeOptions.AutoDelete,
			exchangeOptions.Internal,
			exchangeOptions.NoWait,
			tableToAMQPTable(exchangeOptions.ExchangeArgs),
		)
	}

	return channel.ExchangeDeclarePassive(
		exchangeOptions.Name,
		exchangeOptions.Kind,
		exchangeOptions.Durable,
		exchangeOptions.AutoDelete,
		exchangeOptions.Internal,
		exchangeOptions.NoWait,
		tableToAMQPTable(exchangeOptions.ExchangeArgs),
	)
}
