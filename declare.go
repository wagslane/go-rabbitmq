package rabbitmq

import (
	"fmt"
	"github.com/wagslane/go-rabbitmq/internal/channelmanager"
)

type BindingDestinationType string

const (
	BindingTypeQueue    BindingDestinationType = "queue"
	BindingTypeExchange BindingDestinationType = "exchange"
)

// Binding describes the bhinding of a queue to a routing key on an exchange
type Binding struct {
	BindingOptions
	DestinationType BindingDestinationType
	DestinationName string
	RoutingKey      string
	ExchangeName    string
}

// BindingOptions describes the options a binding can have
type BindingOptions struct {
	NoWait  bool
	Args    Table
	Declare bool
}

func declareQueue(chanManager *channelmanager.ChannelManager, options QueueOptions) error {
	if !options.Declare {
		return nil
	}
	if options.Passive {
		_, err := chanManager.QueueDeclarePassiveSafe(
			options.Name,
			options.Durable,
			options.AutoDelete,
			options.Exclusive,
			options.NoWait,
			tableToAMQPTable(options.Args),
		)
		if err != nil {
			return err
		}
		return nil
	}
	_, err := chanManager.QueueDeclareSafe(
		options.Name,
		options.Durable,
		options.AutoDelete,
		options.Exclusive,
		options.NoWait,
		tableToAMQPTable(options.Args),
	)
	if err != nil {
		return err
	}
	return nil
}

func declareExchange(chanManager *channelmanager.ChannelManager, options ExchangeOptions) error {
	if !options.Declare {
		return nil
	}
	if options.Passive {
		err := chanManager.ExchangeDeclarePassiveSafe(
			options.Name,
			options.Kind,
			options.Durable,
			options.AutoDelete,
			options.Internal,
			options.NoWait,
			tableToAMQPTable(options.Args),
		)
		if err != nil {
			return err
		}
		return nil
	}
	err := chanManager.ExchangeDeclareSafe(
		options.Name,
		options.Kind,
		options.Durable,
		options.AutoDelete,
		options.Internal,
		options.NoWait,
		tableToAMQPTable(options.Args),
	)
	if err != nil {
		return err
	}
	return nil
}

func declareBindings(chanManager *channelmanager.ChannelManager, bindings []Binding) error {
	for _, binding := range bindings {
		if !binding.Declare {
			continue
		}
		if binding.DestinationType == BindingTypeQueue {
			err := chanManager.QueueBindSafe(
				binding.DestinationName,
				binding.RoutingKey,
				binding.ExchangeName,
				binding.NoWait,
				tableToAMQPTable(binding.Args),
			)
			if err != nil {
				return err
			}
		}

		if binding.DestinationType == BindingTypeExchange {
			err := chanManager.ExchangeBindSafe(
				binding.DestinationName,
				binding.RoutingKey,
				binding.ExchangeName,
				binding.NoWait,
				tableToAMQPTable(binding.Args),
			)
			if err != nil {
				return err
			}
		}

	}
	return nil
}

type declareOptions struct {
	Queues    []QueueOptions
	Exchanges []ExchangeOptions
	Bindings  []Binding
}

func declareAll(chanManager *channelmanager.ChannelManager, options declareOptions) error {
	for _, exchangeOptions := range options.Exchanges {
		err := declareExchange(chanManager, exchangeOptions)
		if err != nil {
			return fmt.Errorf("declare exchange failed: %w", err)
		}
	}
	for _, queueOptions := range options.Queues {
		err := declareQueue(chanManager, queueOptions)
		if err != nil {
			return fmt.Errorf("declare queue failed: %w", err)
		}
	}

	err := declareBindings(chanManager, options.Bindings)
	if err != nil {
		return fmt.Errorf("declare bindings failed: %w", err)
	}
	return nil
}
