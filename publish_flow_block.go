package rabbitmq

func (publisher *Publisher) startNotifyFlowHandler() {
	notifyFlowChan := publisher.chanManager.NotifyFlowSafe(make(chan bool))
	publisher.disablePublishDueToFlow.Store(false)

	for ok := range notifyFlowChan {
		if ok {
			publisher.options.Logger.Warnf("pausing publishing due to flow request from server")
		} else {
			publisher.options.Logger.Warnf("resuming publishing due to flow request from server")
		}
		publisher.disablePublishDueToFlow.Store(ok)
	}
}

func (publisher *Publisher) startNotifyBlockedHandler() {
	defer close(publisher.blockedHandlerDone)

	publisher.disablePublishDueToBlocked.Store(false)

	for {
		select {
		case <-publisher.done:
			return
		case b := <-publisher.blockedNotifications:
			if b.Active {
				publisher.options.Logger.Warnf("pausing publishing due to TCP blocking from server")
			} else {
				publisher.options.Logger.Warnf("resuming publishing due to TCP blocking from server")
			}
			publisher.disablePublishDueToBlocked.Store(b.Active)
		}
	}
}
