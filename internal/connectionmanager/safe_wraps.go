package connectionmanager

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// ConsumeSafe safely wraps the (*amqp.Channel).Consume method
func (connManager *ConnectionManager) ConsumeSafe(
	queue,
	consumer string,
	autoAck,
	exclusive,
	noLocal,
	noWait bool,
	args amqp.Table,
) (<-chan amqp.Delivery, error) {
	connManager.channelMux.RLock()
	defer connManager.channelMux.RUnlock()

	return connManager.channel.Consume(
		queue,
		consumer,
		autoAck,
		exclusive,
		noLocal,
		noWait,
		args,
	)
}

// QueueDeclarePassiveSafe safely wraps the (*amqp.Channel).QueueDeclarePassive method
func (connManager *ConnectionManager) QueueDeclarePassiveSafe(
	name string,
	durable bool,
	autoDelete bool,
	exclusive bool,
	noWait bool,
	args amqp.Table,
) (amqp.Queue, error) {
	connManager.channelMux.RLock()
	defer connManager.channelMux.RUnlock()

	return connManager.channel.QueueDeclarePassive(
		name,
		durable,
		autoDelete,
		exclusive,
		noWait,
		args,
	)
}

// QueueDeclareSafe safely wraps the (*amqp.Channel).QueueDeclare method
func (connManager *ConnectionManager) QueueDeclareSafe(
	name string, durable bool, autoDelete bool, exclusive bool, noWait bool, args amqp.Table,
) (amqp.Queue, error) {
	connManager.channelMux.RLock()
	defer connManager.channelMux.RUnlock()

	return connManager.channel.QueueDeclare(
		name,
		durable,
		autoDelete,
		exclusive,
		noWait,
		args,
	)
}

// ExchangeDeclarePassiveSafe safely wraps the (*amqp.Channel).ExchangeDeclarePassive method
func (connManager *ConnectionManager) ExchangeDeclarePassiveSafe(
	name string, kind string, durable bool, autoDelete bool, internal bool, noWait bool, args amqp.Table,
) error {
	connManager.channelMux.RLock()
	defer connManager.channelMux.RUnlock()

	return connManager.channel.ExchangeDeclarePassive(
		name,
		kind,
		durable,
		autoDelete,
		internal,
		noWait,
		args,
	)
}

// ExchangeDeclareSafe safely wraps the (*amqp.Channel).ExchangeDeclare method
func (connManager *ConnectionManager) ExchangeDeclareSafe(
	name string, kind string, durable bool, autoDelete bool, internal bool, noWait bool, args amqp.Table,
) error {
	connManager.channelMux.RLock()
	defer connManager.channelMux.RUnlock()

	return connManager.channel.ExchangeDeclare(
		name,
		kind,
		durable,
		autoDelete,
		internal,
		noWait,
		args,
	)
}

// QueueBindSafe safely wraps the (*amqp.Channel).QueueBind method
func (connManager *ConnectionManager) QueueBindSafe(
	name string, key string, exchange string, noWait bool, args amqp.Table,
) error {
	connManager.channelMux.RLock()
	defer connManager.channelMux.RUnlock()

	return connManager.channel.QueueBind(
		name,
		key,
		exchange,
		noWait,
		args,
	)
}

// QosSafe safely wraps the (*amqp.Channel).Qos method
func (connManager *ConnectionManager) QosSafe(
	prefetchCount int, prefetchSize int, global bool,
) error {
	connManager.channelMux.RLock()
	defer connManager.channelMux.RUnlock()

	return connManager.channel.Qos(
		prefetchCount,
		prefetchSize,
		global,
	)
}

// PublishSafe safely wraps the (*amqp.Channel).Publish method
func (connManager *ConnectionManager) PublishSafe(
	exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing,
) error {
	connManager.channelMux.RLock()
	defer connManager.channelMux.RUnlock()

	return connManager.channel.Publish(
		exchange,
		key,
		mandatory,
		immediate,
		msg,
	)
}

// NotifyReturnSafe safely wraps the (*amqp.Channel).NotifyReturn method
func (connManager *ConnectionManager) NotifyReturnSafe(
	c chan amqp.Return,
) chan amqp.Return {
	connManager.channelMux.RLock()
	defer connManager.channelMux.RUnlock()

	return connManager.channel.NotifyReturn(
		c,
	)
}

// ConfirmSafe safely wraps the (*amqp.Channel).Confirm method
func (connManager *ConnectionManager) ConfirmSafe(
	noWait bool,
) error {
	connManager.channelMux.RLock()
	defer connManager.channelMux.RUnlock()

	return connManager.channel.Confirm(
		noWait,
	)
}

// NotifyPublishSafe safely wraps the (*amqp.Channel).NotifyPublish method
func (connManager *ConnectionManager) NotifyPublishSafe(
	confirm chan amqp.Confirmation,
) chan amqp.Confirmation {
	connManager.channelMux.RLock()
	defer connManager.channelMux.RUnlock()

	return connManager.channel.NotifyPublish(
		confirm,
	)
}

// NotifyFlowSafe safely wraps the (*amqp.Channel).NotifyFlow method
func (connManager *ConnectionManager) NotifyFlowSafe(
	c chan bool,
) chan bool {
	connManager.channelMux.RLock()
	defer connManager.channelMux.RUnlock()

	return connManager.channel.NotifyFlow(
		c,
	)
}

// NotifyBlockedSafe safely wraps the (*amqp.Connection).NotifyBlocked method
func (connManager *ConnectionManager) NotifyBlockedSafe(
	receiver chan amqp.Blocking,
) chan amqp.Blocking {
	connManager.channelMux.RLock()
	defer connManager.channelMux.RUnlock()

	return connManager.connection.NotifyBlocked(
		receiver,
	)
}
