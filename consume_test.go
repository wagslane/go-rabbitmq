package rabbitmq

import (
	"errors"
	"testing"
)

func TestConsumerRestartContinuesAfterError(t *testing.T) {
	reconnectErrCh := make(chan error, 2)
	reconnectErrCh <- errors.New("first reconnect")
	reconnectErrCh <- errors.New("second reconnect")
	close(reconnectErrCh)

	consumer := &Consumer{
		reconnectErrCh: reconnectErrCh,
		options: ConsumerOptions{
			Logger: simpleLogF(t.Logf),
		},
	}
	restarts := 0
	consumer.restartOnReconnect(func() error {
		restarts++
		if restarts == 1 {
			return errors.New("broker failed during declarations")
		}
		return nil
	})

	if restarts != 2 {
		t.Fatalf("restart attempts = %d, want 2", restarts)
	}
}
