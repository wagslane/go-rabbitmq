package dispatcher

import (
	"sync"
)

// Dispatcher fans out reconnect notifications to subscribers
type Dispatcher struct {
	subscribers      map[uint64]dispatchSubscriber
	subscribersMu    sync.Mutex
	nextSubscriberID uint64
}

type dispatchSubscriber struct {
	notifyCancelOrCloseChan chan error
	closeCh                 <-chan struct{}
}

// NewDispatcher -
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		subscribers: make(map[uint64]dispatchSubscriber),
	}
}

// Dispatch sends the error to all subscribers without blocking.
// A subscriber that hasn't consumed its previous notification only keeps
// the latest one; restart logic re-reads current state, so intermediate
// notifications are safe to coalesce.
func (d *Dispatcher) Dispatch(err error) error {
	d.subscribersMu.Lock()
	defer d.subscribersMu.Unlock()
	for _, subscriber := range d.subscribers {
		select {
		case subscriber.notifyCancelOrCloseChan <- err:
		default:
			select {
			case <-subscriber.notifyCancelOrCloseChan:
			default:
			}
			subscriber.notifyCancelOrCloseChan <- err
		}
	}
	return nil
}

// AddSubscriber -
func (d *Dispatcher) AddSubscriber() (<-chan error, chan<- struct{}) {
	closeCh := make(chan struct{})
	notifyCancelOrCloseChan := make(chan error, 1)

	d.subscribersMu.Lock()
	id := d.nextSubscriberID
	d.nextSubscriberID++
	d.subscribers[id] = dispatchSubscriber{
		notifyCancelOrCloseChan: notifyCancelOrCloseChan,
		closeCh:                 closeCh,
	}
	d.subscribersMu.Unlock()

	go func(id uint64) {
		<-closeCh
		d.subscribersMu.Lock()
		defer d.subscribersMu.Unlock()
		sub, ok := d.subscribers[id]
		if !ok {
			return
		}
		close(sub.notifyCancelOrCloseChan)
		delete(d.subscribers, id)
	}(id)
	return notifyCancelOrCloseChan, closeCh
}
