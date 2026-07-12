package dispatcher

import (
	"errors"
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

func TestDispatchDoesNotBlockOnSlowSubscriber(t *testing.T) {
	d := NewDispatcher()
	notifyCh, _ := d.AddSubscriber()

	done := make(chan struct{})
	go func() {
		d.Dispatch(errors.New("first"))
		d.Dispatch(errors.New("second"))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("dispatch blocked on a slow subscriber")
	}

	// a slow subscriber keeps the latest notification
	select {
	case err := <-notifyCh:
		if err.Error() != "second" {
			t.Fatalf("got %q, want latest notification %q", err, "second")
		}
	default:
		t.Fatal("expected a buffered notification")
	}
}
