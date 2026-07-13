package channelmanager

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestLockChannelReadHonorsContext(t *testing.T) {
	manager := &ChannelManager{}
	manager.channelMu.Lock()
	defer manager.channelMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	started := time.Now()
	err := manager.lockChannelRead(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lock error = %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("context cancellation took %s", elapsed)
	}
}

func TestPublishWithContextSafeHonorsContextWhileChannelLocked(t *testing.T) {
	manager := &ChannelManager{}
	manager.channelMu.Lock()
	defer manager.channelMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := manager.PublishWithContextSafe(ctx, "", "key", false, false, amqp.Publishing{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("publish error = %v, want context deadline exceeded", err)
	}
}

func TestPublishWithDeferredConfirmContextSafeHonorsContextWhileChannelLocked(t *testing.T) {
	manager := &ChannelManager{}
	manager.channelMu.Lock()
	defer manager.channelMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	confirmation, err := manager.PublishWithDeferredConfirmWithContextSafe(
		ctx, "", "key", false, false, amqp.Publishing{},
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("publish error = %v, want context deadline exceeded", err)
	}
	if confirmation != nil {
		t.Fatal("confirmation should be nil after context cancellation")
	}
}

func TestLockChannelReadRejectsCanceledContext(t *testing.T) {
	manager := &ChannelManager{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := manager.lockChannelRead(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("lock error = %v, want context canceled", err)
	}
}
