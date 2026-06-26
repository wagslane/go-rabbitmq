package rabbitmq

import (
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// newFlowBlockTestPublisher builds a Publisher with only the fields needed to
// exercise the flow/blocked notification handlers, so the resume logic can be
// tested without a real broker (runs in default CI).
func newFlowBlockTestPublisher() *Publisher {
	return &Publisher{
		disablePublishDueToFlowMu:    &sync.RWMutex{},
		disablePublishDueToBlockedMu: &sync.RWMutex{},
		options:                      getDefaultPublisherOptions(),
		done:                         make(chan struct{}),
	}
}

func (publisher *Publisher) isBlocked() bool {
	publisher.disablePublishDueToBlockedMu.RLock()
	defer publisher.disablePublishDueToBlockedMu.RUnlock()
	return publisher.disablePublishDueToBlocked
}

func (publisher *Publisher) isFlowDisabled() bool {
	publisher.disablePublishDueToFlowMu.RLock()
	defer publisher.disablePublishDueToFlowMu.RUnlock()
	return publisher.disablePublishDueToFlow
}

func eventually(t *testing.T, want bool, get func() bool, msg string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	tkr := time.NewTicker(5 * time.Millisecond)
	defer tkr.Stop()
	for {
		if get() == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("%s: timed out waiting for state to become %v", msg, want)
		case <-tkr.C:
		}
	}
}

// TestPublisherResumesOnUnblock is the ENG-1846 regression test: after a
// connection.blocked pauses publishing, a connection.unblocked must clear the
// paused state so publishing resumes — without a reconnect.
func TestPublisherResumesOnUnblock(t *testing.T) {
	publisher := newFlowBlockTestPublisher()
	blockings := make(chan amqp.Blocking, 1)

	resubscribe := make(chan bool, 1)
	go func() { resubscribe <- publisher.consumeBlockings(blockings) }()

	if publisher.isBlocked() {
		t.Fatal("publisher should start unblocked")
	}

	// Broker blocks: publishing pauses.
	blockings <- amqp.Blocking{Active: true, Reason: "low on memory"}
	eventually(t, true, publisher.isBlocked, "after connection.blocked")

	// Broker unblocks: publishing must resume.
	blockings <- amqp.Blocking{Active: false}
	eventually(t, false, publisher.isBlocked, "after connection.unblocked")

	// Connection drop closes the receiver: the handler asks to re-subscribe.
	close(blockings)
	select {
	case got := <-resubscribe:
		if !got {
			t.Fatal("expected consumeBlockings to request re-subscribe when receiver closes")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("consumeBlockings did not return after receiver closed")
	}
}

// TestPublisherConsumeBlockingsStopsOnClose verifies the handler exits (does not
// re-subscribe) when the publisher itself is closed.
func TestPublisherConsumeBlockingsStopsOnClose(t *testing.T) {
	publisher := newFlowBlockTestPublisher()
	blockings := make(chan amqp.Blocking)

	resubscribe := make(chan bool, 1)
	go func() { resubscribe <- publisher.consumeBlockings(blockings) }()

	close(publisher.done)
	select {
	case got := <-resubscribe:
		if got {
			t.Fatal("expected consumeBlockings to stop (not re-subscribe) on publisher close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("consumeBlockings did not return after publisher close")
	}
}

// TestPublisherResumesOnFlow mirrors the unblock test for channel-level flow
// control (basic.flow).
func TestPublisherResumesOnFlow(t *testing.T) {
	publisher := newFlowBlockTestPublisher()
	flow := make(chan bool, 1)

	resubscribe := make(chan bool, 1)
	go func() { resubscribe <- publisher.consumeFlow(flow) }()

	flow <- true
	eventually(t, true, publisher.isFlowDisabled, "after basic.flow active")

	flow <- false
	eventually(t, false, publisher.isFlowDisabled, "after basic.flow inactive")

	close(flow)
	select {
	case got := <-resubscribe:
		if !got {
			t.Fatal("expected consumeFlow to request re-subscribe when receiver closes")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("consumeFlow did not return after receiver closed")
	}
}
