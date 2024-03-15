package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

func (publisher *Publisher) startNotifyFlowHandler() {
	notifyFlowChan := publisher.chanManager.NotifyFlowSafe(make(chan bool))
	publisher.disablePublishDueToFlowMux.Lock()
	publisher.disablePublishDueToFlow = false
	publisher.disablePublishDueToFlowMux.Unlock()

	for ok := range notifyFlowChan {
		publisher.disablePublishDueToFlowMux.Lock()
		if ok {
			publisher.disablePublishDueToFlow = true
		} else {
			publisher.disablePublishDueToFlow = false
		}
		publisher.disablePublishDueToFlowMux.Unlock()
	}
}

func (publisher *Publisher) startNotifyBlockedHandler() {
	blockings := publisher.connManager.NotifyBlockedSafe(make(chan amqp.Blocking))
	publisher.disablePublishDueToBlockedMux.Lock()
	publisher.disablePublishDueToBlocked = false
	publisher.disablePublishDueToBlockedMux.Unlock()

	for b := range blockings {
		publisher.disablePublishDueToBlockedMux.Lock()
		if b.Active {
			publisher.disablePublishDueToBlocked = true
		} else {
			publisher.disablePublishDueToBlocked = false
		}
		publisher.disablePublishDueToBlockedMux.Unlock()
	}
}
