package connectionmanager

import amqp "github.com/rabbitmq/amqp091-go"

func (connManager *ConnectionManager) startNotifyBlocked(conn *amqp.Connection) {
	blockings := conn.NotifyBlocked(make(chan amqp.Blocking, 1))
	go func() {
		for blocking := range blockings {
			connManager.dispatchBlocked(blocking)
		}
	}()
}

func (connManager *ConnectionManager) SubscribeBlocked() (<-chan amqp.Blocking, func()) {
	receiver := make(chan amqp.Blocking, 1)

	connManager.blockedSubscribersMu.Lock()
	id := connManager.nextBlockedSubscriberID
	connManager.nextBlockedSubscriberID++
	connManager.blockedSubscribers[id] = receiver
	connManager.blockedSubscribersMu.Unlock()

	return receiver, func() {
		connManager.blockedSubscribersMu.Lock()
		delete(connManager.blockedSubscribers, id)
		connManager.blockedSubscribersMu.Unlock()
	}
}

func (connManager *ConnectionManager) dispatchBlocked(blocking amqp.Blocking) {
	connManager.blockedSubscribersMu.Lock()
	defer connManager.blockedSubscribersMu.Unlock()

	for _, receiver := range connManager.blockedSubscribers {
		select {
		case receiver <- blocking:
		default:
			// Keep the latest connection state when a subscriber is behind.
			select {
			case <-receiver:
			default:
			}
			receiver <- blocking
		}
	}
}
