package connectionmanager

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// NotifyBlockedSafe safely wraps the (*amqp.Connection).NotifyBlocked method
func (connManager *ConnectionManager) NotifyBlockedSafe(
	receiver chan amqp.Blocking,
) chan amqp.Blocking {
	connManager.connectionMu.Lock()
	defer connManager.connectionMu.Unlock()

	// add receiver to connection manager.
	connManager.publisherNotifyBlockingReceiversMu.Lock()
	connManager.publisherNotifyBlockingReceivers = append(connManager.publisherNotifyBlockingReceivers, receiver)
	connManager.publisherNotifyBlockingReceiversMu.Unlock()

	if !connManager.universalNotifyBlockingReceiverUsed {
		connManager.connection.NotifyBlocked(
			connManager.universalNotifyBlockingReceiver,
		)
		connManager.universalNotifyBlockingReceiverUsed = true
	}

	return receiver
}

// readUniversalBlockReceiver reads on universal blocking receiver and broadcasts event to all blocking receivers of
// connection manager.
func (connManager *ConnectionManager) readUniversalBlockReceiver() {
	for b := range connManager.universalNotifyBlockingReceiver {
		connManager.publisherNotifyBlockingReceiversMu.RLock()
		for _, br := range connManager.publisherNotifyBlockingReceivers {
			br <- b
		}
		connManager.publisherNotifyBlockingReceiversMu.RUnlock()
	}
}

func (connManager *ConnectionManager) RemovePublisherBlockingReceiver(receiver chan amqp.Blocking) {
	connManager.publisherNotifyBlockingReceiversMu.Lock()
	for i, br := range connManager.publisherNotifyBlockingReceivers {
		if br == receiver {
			connManager.publisherNotifyBlockingReceivers = append(connManager.publisherNotifyBlockingReceivers[:i], connManager.publisherNotifyBlockingReceivers[i+1:]...)
		}
	}
	connManager.publisherNotifyBlockingReceiversMu.Unlock()
	close(receiver)
}
