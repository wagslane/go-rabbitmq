package dispatcher

import (
	"log"
	"math"
	"math/rand"
	"sync"
	"time"
)

// Dispatcher -
type Dispatcher struct {
	subscribers    map[int]dispatchSubscriber
	subscribersMux *sync.Mutex
}

type dispatchSubscriber struct {
	notifyClosedChan        chan error
	notifyCancelOrCloseChan chan error
	closeCh                 <-chan struct{}
}

// NewDispatcher -
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		subscribers:    make(map[int]dispatchSubscriber),
		subscribersMux: &sync.Mutex{},
	}
}

// Dispatch -
func (d *Dispatcher) Dispatch(err error) error {
	d.subscribersMux.Lock()
	defer d.subscribersMux.Unlock()

	for _, subscriber := range d.subscribers {
		select {
		case <-time.After(time.Second * 5):
			log.Println("Unexpected rabbitmq error: timeout in dispatch")

		case subscriber.notifyCancelOrCloseChan <- err:
		}
	}

	return nil
}

// DispathLooseConnection dispatching that connection to RabbitMQ is loosed
func (d *Dispatcher) DispathLooseConnection(err error) error {
	d.subscribersMux.Lock()
	defer d.subscribersMux.Unlock()

	for _, subscriber := range d.subscribers {
		subscriber.notifyClosedChan <- err
	}

	return nil
}

// AddSubscriber -
func (d *Dispatcher) AddSubscriber() (<-chan error, chan<- struct{}, <-chan error) {
	const maxRand = math.MaxInt
	const minRand = 0
	id := rand.Intn(maxRand-minRand) + minRand

	closeCh := make(chan struct{})
	notifyCancelOrCloseChan := make(chan error)
	notifyClosedChan := make(chan error)

	d.subscribersMux.Lock()

	d.subscribers[id] = dispatchSubscriber{
		notifyClosedChan:        notifyClosedChan,
		notifyCancelOrCloseChan: notifyCancelOrCloseChan,
		closeCh:                 closeCh,
	}

	d.subscribersMux.Unlock()

	go func(id int) {
		<-closeCh
		d.subscribersMux.Lock()
		defer d.subscribersMux.Unlock()
		sub, ok := d.subscribers[id]
		if !ok {
			return
		}
		close(sub.notifyCancelOrCloseChan)
		delete(d.subscribers, id)
	}(id)

	return notifyCancelOrCloseChan, closeCh, notifyClosedChan
}
