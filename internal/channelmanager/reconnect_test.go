package channelmanager

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/wagslane/go-rabbitmq/internal/connectionmanager"
)

// prepareChannelManagerBroker launches a rabbitmq container for this package's
// integration tests. It publishes on host port 5673 so it cannot collide with
// the root package's integration broker on 5672 when `go test ./...` runs the
// package test binaries in parallel.
func prepareChannelManagerBroker(t *testing.T) string {
	const flag = "ENABLE_DOCKER_INTEGRATION_TESTS"
	if v, ok := os.LookupEnv(flag); !ok || strings.ToUpper(v) != "TRUE" {
		t.Skipf("integration tests are only run if '%s' is TRUE", flag)
		return ""
	}
	out, err := exec.Command("docker", "run", "--rm", "--detach", "--publish=5673:5672", "--quiet", "--", "rabbitmq:4.1.1-alpine").Output()
	if err != nil {
		t.Log("container id", string(out))
		t.Fatalf("error launching rabbitmq in docker: %v", err)
	}
	t.Cleanup(func() {
		containerId := strings.TrimSpace(string(out))
		t.Logf("attempting to shutdown container '%s'", containerId)
		if err := exec.Command("docker", "rm", "--force", containerId).Run(); err != nil {
			t.Logf("failed to stop: %v", err)
		}
	})
	return "amqp://guest:guest@localhost:5673/"
}

type staticResolver []string

func (r staticResolver) Resolve() ([]string, error) { return r, nil }

// discardLogger swallows manager logs: the reconnect goroutines outlive the
// test body, and logging through testing.T after the test ends panics.
type discardLogger struct{}

func (discardLogger) Fatalf(string, ...interface{}) {}
func (discardLogger) Errorf(string, ...interface{}) {}
func (discardLogger) Warnf(string, ...interface{})  {}
func (discardLogger) Infof(string, ...interface{})  {}
func (discardLogger) Debugf(string, ...interface{}) {}

// TestReconnectionCountCoupledToChannelSwap guards the coupling that
// PublishWithOutcome's returns-listener gate depends on: the reconnection
// count advances before a swapped-in channel becomes reachable through
// channelMu. Probers snapshot (channel, count) under the read lock across
// repeated forced reconnects; a snapshot pair that disagrees on the channel
// but agrees on the count means a publish could have gone out on a channel
// the outcome tracker's returns listener does not cover. Because the broken
// window is only a few instructions wide, the probe is complemented by a
// deterministic check: reconnect() holds channelMu for its whole body, so
// requiring the count to have advanced by the time it returns pins the
// increment inside the critical section.
func TestReconnectionCountCoupledToChannelSwap(t *testing.T) {
	url := prepareChannelManagerBroker(t)

	log := discardLogger{}
	var connManager *connectionmanager.ConnectionManager
	deadline := time.Now().Add(30 * time.Second)
	for {
		var err error
		connManager, err = connectionmanager.NewConnectionManager(staticResolver{url}, amqp.Config{}, log, time.Second)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("broker did not become ready: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	defer connManager.Close()

	chanManager, err := NewChannelManager(connManager, log, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("creating channel manager: %v", err)
	}
	defer chanManager.Close()

	stop := make(chan struct{})
	violation := make(chan string, 1)
	var probers sync.WaitGroup
	for i := 0; i < 4; i++ {
		probers.Add(1)
		go func() {
			defer probers.Done()
			var lastChannel *amqp.Channel
			var lastCount uint
			for {
				select {
				case <-stop:
					return
				default:
				}
				chanManager.channelMu.RLock()
				ch := chanManager.channel
				count := chanManager.GetReconnectionCount()
				chanManager.channelMu.RUnlock()
				if lastChannel != nil && ch != lastChannel && count == lastCount {
					select {
					case violation <- fmt.Sprintf("new channel reachable while the reconnection count still reads %d", count):
					default:
					}
					return
				}
				lastChannel, lastCount = ch, count
				runtime.Gosched()
			}
		}()
	}

	for i := 0; i < 15; i++ {
		before := chanManager.GetReconnectionCount()
		// publishing to a missing exchange fails the channel with a 404,
		// forcing a reconnect; the publish error itself is irrelevant
		_ = chanManager.PublishWithContextSafe(
			context.Background(),
			"no-such-exchange-gorabbitmq-test", "key", false, false,
			amqp.Publishing{Body: []byte("boom")},
		)
		reconnected := time.Now().Add(10 * time.Second)
		for chanManager.GetReconnectionCount() == before {
			if time.Now().After(reconnected) {
				t.Fatalf("reconnect %d did not happen within 10s", i)
			}
			time.Sleep(time.Millisecond)
		}
	}

	close(stop)
	probers.Wait()
	select {
	case msg := <-violation:
		t.Fatal(msg)
	default:
	}

	// the deterministic half: reconnect() must advance the count itself,
	// under the channel lock, not leave it to a caller after the unlock.
	// (Closing the old channel gracefully does not trigger the manager's own
	// reconnect loop, so the +1 below can come only from reconnect itself.)
	before := chanManager.GetReconnectionCount()
	if err := chanManager.reconnect(); err != nil {
		t.Fatalf("direct reconnect: %v", err)
	}
	if got := chanManager.GetReconnectionCount(); got != before+1 {
		t.Fatalf("reconnection count after reconnect() = %d, want %d: the count must advance inside reconnect's critical section, before the new channel is reachable", got, before+1)
	}
}
