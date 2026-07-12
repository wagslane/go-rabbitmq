package rabbitmq

import (
	"errors"
	"testing"
)

func TestPublisherRestartContinuesAfterError(t *testing.T) {
	reconnectErrCh := make(chan error, 2)
	reconnectErrCh <- errors.New("first reconnect")
	reconnectErrCh <- errors.New("second reconnect")
	close(reconnectErrCh)

	publisher := &Publisher{
		reconnectErrCh: reconnectErrCh,
		options: PublisherOptions{
			Logger: simpleLogF(t.Logf),
		},
	}
	restarts := 0
	publisher.restartOnReconnect(func() error {
		restarts++
		if restarts == 1 {
			return errors.New("broker failed during exchange declaration")
		}
		return nil
	})

	if restarts != 2 {
		t.Fatalf("restart attempts = %d, want 2", restarts)
	}
}

func TestPublishFailsFastWhenPaused(t *testing.T) {
	publisher := &Publisher{}

	publisher.disablePublishDueToFlow.Store(true)
	if err := publisher.Publish([]byte{}, []string{"key"}); err == nil {
		t.Fatal("expected an error while paused due to flow")
	}
	publisher.disablePublishDueToFlow.Store(false)

	publisher.disablePublishDueToBlocked.Store(true)
	if err := publisher.Publish([]byte{}, []string{"key"}); err == nil {
		t.Fatal("expected an error while paused due to TCP block")
	}
}
