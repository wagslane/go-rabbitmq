package connectionmanager

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func newBlockedTestManager() *ConnectionManager {
	return &ConnectionManager{
		blockedSubscribers: make(map[uint64]chan amqp.Blocking),
	}
}

func TestSubscribeBlockedUnsubscribe(t *testing.T) {
	manager := newBlockedTestManager()
	_, unsubscribe := manager.SubscribeBlocked()

	if got := len(manager.blockedSubscribers); got != 1 {
		t.Fatalf("blocked subscribers = %d, want 1", got)
	}

	unsubscribe()
	if got := len(manager.blockedSubscribers); got != 0 {
		t.Fatalf("blocked subscribers = %d, want 0", got)
	}
}

func TestDispatchBlockedKeepsLatestState(t *testing.T) {
	manager := newBlockedTestManager()
	notifications, unsubscribe := manager.SubscribeBlocked()
	defer unsubscribe()

	manager.dispatchBlocked(amqp.Blocking{Active: true})
	manager.dispatchBlocked(amqp.Blocking{Active: false})

	blocking := <-notifications
	if blocking.Active {
		t.Fatal("blocked notification did not retain latest state")
	}
}
