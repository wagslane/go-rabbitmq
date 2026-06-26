package rabbitmq

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// minResubscribeWait is the minimum pause between re-subscription attempts when the
// underlying notification channel closes (i.e. the channel/connection dropped).
// It avoids a tight spin while the connection manager is reconnecting.
const minResubscribeWait = 500 * time.Millisecond

// startNotifyFlowHandler keeps a subscription to channel-level flow control
// (basic.flow) alive for the whole lifetime of the publisher. When the
// underlying amqp channel is recreated on reconnect its flow channel is closed,
// so we re-subscribe on the new channel rather than letting the handler die.
func (publisher *Publisher) startNotifyFlowHandler() {
	for {
		select {
		case <-publisher.done:
			return
		default:
		}

		// NotifyFlowSafe blocks until any in-progress channel reconnect releases
		// the channel lock, so we always register against the live channel. The
		// receiver is buffered so the amqp reader goroutine is never blocked
		// delivering a flow event to us.
		notifyFlowChan := publisher.chanManager.NotifyFlowSafe(make(chan bool, 1))
		publisher.setFlow(false)

		if !publisher.consumeFlow(notifyFlowChan) {
			return // publisher closed
		}

		// Channel closed (reconnect in progress). Pause before re-subscribing.
		if !publisher.waitBeforeResubscribe() {
			return
		}
	}
}

// startNotifyBlockedHandler keeps a subscription to connection-level TCP flow
// control (connection.blocked / connection.unblocked) alive for the whole
// lifetime of the publisher. The receiver is registered against the connection,
// which is only closed when the connection itself drops, so we re-subscribe on
// the new connection after a reconnect.
//
// The receiver MUST be buffered: amqp091-go delivers blocked/unblocked events
// synchronously from the single connection-reader goroutine, fanning out to
// every registered receiver with a blocking send. An unbuffered (or full)
// receiver therefore wedges the reader, which then never processes the
// connection.unblocked frame — leaving every publisher on the connection paused
// forever. That wedge is the root cause of ENG-1846.
func (publisher *Publisher) startNotifyBlockedHandler() {
	for {
		select {
		case <-publisher.done:
			return
		default:
		}

		blockings := publisher.connManager.NotifyBlockedSafe(make(chan amqp.Blocking, 1))
		publisher.setBlocked(false)

		if !publisher.consumeBlockings(blockings) {
			return // publisher closed
		}

		// Receiver closed (connection dropped). Pause before re-subscribing on
		// the reconnected connection.
		if !publisher.waitBeforeResubscribe() {
			return
		}
	}
}

// consumeFlow drains flow notifications until either the publisher is closed
// (returns false) or the receiver is closed by a reconnect (returns true,
// signalling the caller to re-subscribe).
func (publisher *Publisher) consumeFlow(notifyFlowChan <-chan bool) (resubscribe bool) {
	for {
		select {
		case <-publisher.done:
			return false
		case ok, open := <-notifyFlowChan:
			if !open {
				return true
			}
			if ok {
				publisher.options.Logger.Warnf("pausing publishing due to flow request from server")
			} else {
				publisher.options.Logger.Warnf("resuming publishing due to flow request from server")
			}
			publisher.setFlow(ok)
		}
	}
}

// consumeBlockings drains connection.blocked / connection.unblocked
// notifications until either the publisher is closed (returns false) or the
// receiver is closed by a reconnect (returns true, signalling the caller to
// re-subscribe).
func (publisher *Publisher) consumeBlockings(blockings <-chan amqp.Blocking) (resubscribe bool) {
	for {
		select {
		case <-publisher.done:
			return false
		case b, open := <-blockings:
			if !open {
				return true
			}
			if b.Active {
				publisher.options.Logger.Warnf("pausing publishing due to TCP blocking from server")
			} else {
				publisher.options.Logger.Warnf("resuming publishing due to TCP blocking from server")
			}
			publisher.setBlocked(b.Active)
		}
	}
}

// setFlow updates whether publishing is disabled due to a channel flow request.
func (publisher *Publisher) setFlow(disabled bool) {
	publisher.disablePublishDueToFlowMu.Lock()
	defer publisher.disablePublishDueToFlowMu.Unlock()
	publisher.disablePublishDueToFlow = disabled
}

// setBlocked updates whether publishing is disabled due to a connection block.
func (publisher *Publisher) setBlocked(disabled bool) {
	publisher.disablePublishDueToBlockedMu.Lock()
	defer publisher.disablePublishDueToBlockedMu.Unlock()
	publisher.disablePublishDueToBlocked = disabled
}

// waitBeforeResubscribe pauses between re-subscription attempts, returning false
// if the publisher is closed while waiting.
func (publisher *Publisher) waitBeforeResubscribe() (keepGoing bool) {
	wait := max(publisher.connManager.ReconnectInterval, minResubscribeWait)
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-publisher.done:
		return false
	case <-timer.C:
		return true
	}
}
