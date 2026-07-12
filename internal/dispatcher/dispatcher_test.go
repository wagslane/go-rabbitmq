package dispatcher

import (
	"testing"
	"time"
)

func TestNewDispatcher(t *testing.T) {
	d := NewDispatcher()
	if d.subscribers == nil {
		t.Error("Dispatcher subscribers is nil")
	}
	if d.subscribersMu == nil {
		t.Error("Dispatcher subscribersMu is nil")
	}
}

func TestAddSubscriber(t *testing.T) {
	d := NewDispatcher()
	d.AddSubscriber()
	if len(d.subscribers) != 1 {
		t.Error("Dispatcher subscribers length is not 1")
	}
}

func TestCloseSubscriber(t *testing.T) {
	d := NewDispatcher()
	notifyCh, closeCh := d.AddSubscriber()
	close(closeCh)
	select {
	case <-notifyCh:
	case <-time.After(time.Second):
		t.Fatal("Subscriber notification channel was not closed")
	}

	d.subscribersMu.Lock()
	defer d.subscribersMu.Unlock()
	if len(d.subscribers) != 0 {
		t.Error("Dispatcher subscribers length is not 0")
	}
}
