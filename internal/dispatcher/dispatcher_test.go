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
	if d.subscribersMux == nil {
		t.Error("Dispatcher subscribersMux is nil")
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
	_, closeCh := d.AddSubscriber()
	close(closeCh)
	time.Sleep(time.Millisecond)
	if len(d.subscribers) != 0 {
		t.Error("Dispatcher subscribers length is not 0")
	}
}
