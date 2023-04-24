package rabbitmq

import (
	"errors"

	"github.com/DizoftTeam/go-rabbitmq/internal/channelmanager"
)

type Declarator struct {
	chanManager *channelmanager.ChannelManager
}

func NewDeclarator(conn *Conn) (*Declarator, error) {
	if conn.connectionManager == nil {
		return nil, errors.New("connection manager can't be nil")
	}

	chanManager, err := channelmanager.NewChannelManager(conn.connectionManager, &stdDebugLogger{}, conn.connectionManager.ReconnectInterval)
	if err != nil {
		return nil, err
	}

	result := &Declarator{
		chanManager: chanManager,
	}

	return result, nil
}

func (d *Declarator) Close() {
	d.chanManager.Close()
}

func (d *Declarator) Declare(bindings []ExchangeBinding) error {
	for _, binding := range bindings {
		err := d.chanManager.ExchangeBindSafe(
			binding.From,
			binding.To,
			binding.RoutingKey,
			binding.NoWait,
			tableToAMQPTable(binding.Args),
		)

		if err != nil {
			return err
		}
	}

	return nil
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

func declareBindings(chanManager *channelmanager.ChannelManager, options ConsumerOptions) error {
	for _, binding := range options.Bindings {
		if !binding.Declare {
			continue
		}

		err := chanManager.QueueBindSafe(
			options.QueueOptions.Name,
			binding.RoutingKey,
			options.ExchangeOptions.Name,
			binding.NoWait,
			tableToAMQPTable(binding.Args),
		)

		if err != nil {
			return err
		}
	}

	return nil
}
