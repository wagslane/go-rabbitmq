package dispatcher

import (
	"testing"
	"time"
)

type lgr struct{}

func (l *lgr) Fatalf(string, ...interface{}) {}
func (l *lgr) Errorf(string, ...interface{}) {}
func (l *lgr) Warnf(string, ...interface{})  {}
func (l *lgr) Infof(string, ...interface{})  {}
func (l *lgr) Debugf(string, ...interface{}) {}

func TestNewDispatcher(t *testing.T) {
	d := NewDispatcher(&lgr{})
	if d.subscribers == nil {
		t.Error("Dispatcher subscribers is nil")
	}
	if d.subscribersMux == nil {
		t.Error("Dispatcher subscribersMux is nil")
	}
}

func TestAddSubscriber(t *testing.T) {
	d := NewDispatcher(&lgr{})
	d.AddSubscriber()
	if len(d.subscribers) != 1 {
		t.Error("Dispatcher subscribers length is not 1")
	}
}

func TestCloseSubscriber(t *testing.T) {
	d := NewDispatcher(&lgr{})
	_, closeCh := d.AddSubscriber()
	close(closeCh)
	time.Sleep(time.Millisecond)
	if len(d.subscribers) != 0 {
		t.Error("Dispatcher subscribers length is not 0")
	}
}
