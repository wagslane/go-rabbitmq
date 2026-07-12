package rabbitmq

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const enableDockerIntegrationTestsFlag = `ENABLE_DOCKER_INTEGRATION_TESTS`

func prepareDockerTest(t *testing.T) (connStr string) {
	if v, ok := os.LookupEnv(enableDockerIntegrationTestsFlag); !ok || strings.ToUpper(v) != "TRUE" {
		t.Skipf("integration tests are only run if '%s' is TRUE", enableDockerIntegrationTestsFlag)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out, err := exec.CommandContext(ctx, "docker", "run", "--rm", "--detach", "--publish=5672:5672", "--quiet", "--", "rabbitmq:4.1.1-alpine").Output()
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
	return "amqp://guest:guest@localhost:5672/"
}

func waitForHealthyAmqp(t *testing.T, connStr string, optionFuncs ...func(*ConnectionOptions)) *Conn {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tkr := time.NewTicker(time.Second)

	// only log connection-level logs when connection has succeeded;
	// atomic because connection goroutines log concurrently with the test
	var muted atomic.Bool
	muted.Store(true)
	connLogger := simpleLogF(func(s string, i ...interface{}) {
		if !muted.Load() {
			t.Logf(s, i...)
		}
	})

	var lastErr error
	for {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for healthy amqp", lastErr)
			return nil
		case <-tkr.C:
			t.Log("attempting connection")
			options := append(optionFuncs, WithConnectionOptionsLogger(connLogger))
			conn, err := NewConn(connStr, options...)
			if err != nil {
				lastErr = err
				t.Log("connection attempt failed - retrying")
			} else {
				if err := func() error {
					pub, err := NewPublisher(conn, WithPublisherOptionsLogger(simpleLogF(t.Logf)))
					if err != nil {
						return fmt.Errorf("failed to setup publisher: %v", err)
					}
					t.Log("attempting publish")
					defer pub.Close()
					return pub.PublishWithContext(ctx, []byte{}, []string{"ping"}, WithPublishOptionsExchange(""))
				}(); err != nil {
					_ = conn.Close()
					t.Log("publish ping failed", err.Error())
				} else {
					t.Log("ping successful")
					muted.Store(true)
					return conn
				}
			}
		}
	}
}

// TestSimplePubSub is an integration testing function that validates whether we can reliably connect to a docker-based
// rabbitmq and consumer a message that we publish. This uses the default direct exchange with lots of error checking
// to ensure the result is as expected.
func TestSimplePubSub(t *testing.T) {
	connStr := prepareDockerTest(t)
	conn := waitForHealthyAmqp(t, connStr)
	defer conn.Close()

	t.Logf("new consumer")
	consumerQueue := "my_queue"
	consumer, err := NewConsumer(conn, consumerQueue, WithConsumerOptionsLogger(simpleLogF(t.Logf)))
	if err != nil {
		t.Fatal("error creating consumer", err)
	}
	defer consumer.CloseWithContext(context.Background())

	// Setup a consumer which pushes each of its consumed messages over the channel. If the channel is closed or full
	// it does not block.
	consumed := make(chan Delivery)
	defer close(consumed)

	go func() {
		err = consumer.Run(func(d Delivery) Action {
			t.Log("consumed")
			select {
			case consumed <- d:
			default:
			}
			return Ack
		})
		if err != nil {
			t.Log("consumer run failed", err)
		}
	}()

	// Setup a publisher with notifications enabled
	t.Logf("new publisher")
	publisher, err := NewPublisher(conn, WithPublisherOptionsLogger(simpleLogF(t.Logf)))
	if err != nil {
		t.Fatal("error creating publisher", err)
	}
	publisher.NotifyPublish(func(p Confirmation) {
	})
	defer publisher.Close()

	// For test stability we cannot rely on the fact that the consumer go routines are up and running before the
	// publisher starts it's first publish attempt. For this reason we run the publisher in a loop every second and
	// pass after we see the first message come through.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tkr := time.NewTicker(time.Second)
	for {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for pub sub", ctx.Err())
		case <-tkr.C:
			t.Logf("new publish")
			confirms, err := publisher.PublishWithDeferredConfirmWithContext(ctx, []byte("example"), []string{consumerQueue})
			if err != nil {
				// publish should always succeed since we've verified the ping previously
				t.Fatal("failed to publish", err)
			}
			for _, confirm := range confirms {
				if _, err := confirm.WaitContext(ctx); err != nil {
					t.Fatal("failed to wait for publish", err)
				}
			}
		case d := <-consumed:
			t.Logf("successfully saw message round trip: '%s'", string(d.Body))
			return
		}
	}
}

func TestPublisherCloseReleasesBlockedHandler(t *testing.T) {
	connStr := prepareDockerTest(t)
	conn := waitForHealthyAmqp(t, connStr)
	defer conn.Close()

	closedPublisher, err := NewPublisher(conn, WithPublisherOptionsLogger(simpleLogF(t.Logf)))
	if err != nil {
		t.Fatal("error creating publisher", err)
	}
	activePublisher, err := NewPublisher(conn, WithPublisherOptionsLogger(simpleLogF(t.Logf)))
	if err != nil {
		t.Fatal("error creating second publisher", err)
	}
	defer activePublisher.Close()

	closedPublisher.Close()
	select {
	case <-closedPublisher.blockedHandlerDone:
	case <-time.After(time.Second):
		t.Fatal("publisher blocked handler did not stop after close")
	}

	if err := activePublisher.Publish([]byte("still connected"), []string{"unused"}); err != nil {
		t.Fatalf("second publisher failed after first publisher closed: %v", err)
	}
}

func TestPublisherRestoresConfirmModeBeforeReconnectCompletes(t *testing.T) {
	connStr := prepareDockerTest(t)
	conn := waitForHealthyAmqp(t, connStr, WithConnectionOptionsReconnectInterval(10*time.Millisecond))
	defer conn.Close()

	tests := []struct {
		name    string
		options []func(*PublisherOptions)
		dynamic bool
	}{
		{name: "configured", options: []func(*PublisherOptions){WithPublisherOptionsConfirm}},
		{name: "dynamic", dynamic: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			publisher, err := NewPublisher(conn, test.options...)
			if err != nil {
				t.Fatal("error creating publisher", err)
			}
			defer publisher.Close()
			if test.dynamic {
				publisher.NotifyPublish(func(Confirmation) {})
			}

			for i := 0; i < 3; i++ {
				reconnectionCount := publisher.chanManager.GetReconnectionCount()
				_ = publisher.Publish(
					[]byte("close channel"),
					[]string{"unused"},
					WithPublishOptionsExchange(fmt.Sprintf("missing-%d", i)),
				)

				deadline := time.Now().Add(2 * time.Second)
				for publisher.chanManager.GetReconnectionCount() == reconnectionCount {
					if time.Now().After(deadline) {
						t.Fatal("timed out waiting for channel reconnect")
					}
					runtime.Gosched()
				}

				confirmations, err := publisher.PublishWithDeferredConfirmWithContext(
					context.Background(),
					[]byte("confirmed"),
					[]string{"unused"},
				)
				if err != nil {
					t.Fatal("publish after reconnect failed", err)
				}
				if len(confirmations) != 1 || confirmations[0] == nil {
					t.Fatal("reconnected channel was published before confirm mode was restored")
				}
			}
		})
	}
}

func TestPublisherConfirmationsInOrder(t *testing.T) {
	const messageCount = 50

	connStr := prepareDockerTest(t)
	conn := waitForHealthyAmqp(t, connStr)
	defer conn.Close()

	publisher, err := NewPublisher(conn, WithPublisherOptionsLogger(simpleLogF(t.Logf)), WithPublisherOptionsConfirm)
	if err != nil {
		t.Fatal("error creating publisher", err)
	}
	defer publisher.Close()

	var tagsMu sync.Mutex
	var tags []uint64
	collect := func(c Confirmation) {
		tagsMu.Lock()
		tags = append(tags, c.DeliveryTag)
		tagsMu.Unlock()
	}
	publisher.NotifyPublish(collect)

	for i := 0; i < messageCount; i++ {
		if i == messageCount/2 {
			// swap the handler while confirmations are in flight
			publisher.NotifyPublish(collect)
		}
		if err := publisher.Publish([]byte("ordered"), []string{"unrouted"}); err != nil {
			t.Fatal("publish failed", err)
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		tagsMu.Lock()
		count := len(tags)
		tagsMu.Unlock()
		if count >= messageCount {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for confirmations: got %d of %d", count, messageCount)
		}
		time.Sleep(10 * time.Millisecond)
	}

	tagsMu.Lock()
	defer tagsMu.Unlock()
	for i := 1; i < len(tags); i++ {
		if tags[i] <= tags[i-1] {
			t.Fatalf("confirmations out of order at index %d: tag %d after tag %d", i, tags[i], tags[i-1])
		}
	}
}

func TestNotifyPublishSurfacesConfirmError(t *testing.T) {
	connStr := prepareDockerTest(t)
	conn := waitForHealthyAmqp(t, connStr)
	defer conn.Close()

	var logMu sync.Mutex
	var logs []string
	logger := simpleLogF(func(format string, args ...interface{}) {
		logMu.Lock()
		defer logMu.Unlock()
		logs = append(logs, fmt.Sprintf(format, args...))
	})

	publisher, err := NewPublisher(conn, WithPublisherOptionsLogger(logger))
	if err != nil {
		t.Fatal("error creating publisher", err)
	}

	// confirm mode can't be established on the closed channel
	publisher.Close()

	publisher.NotifyPublish(func(Confirmation) {})

	logMu.Lock()
	defer logMu.Unlock()
	for _, line := range logs {
		if strings.Contains(line, "could not put channel in confirm mode") {
			return
		}
	}
	t.Fatalf("expected a confirm mode error to be logged, got logs: %q", logs)
}

// TestConnCloseDuringReconnectStaysClosed closes a connection while its
// reconnect loop is dialing a dead broker, then brings the broker back:
// the closed connection must never reconnect.
func TestConnCloseDuringReconnectStaysClosed(t *testing.T) {
	if v, ok := os.LookupEnv(enableDockerIntegrationTestsFlag); !ok || strings.ToUpper(v) != "TRUE" {
		t.Skipf("integration tests are only run if '%s' is TRUE", enableDockerIntegrationTestsFlag)
	}
	const hostPort = "5673"
	connStr := fmt.Sprintf("amqp://guest:guest@localhost:%s/", hostPort)

	runBroker := func() string {
		out, err := exec.Command("docker", "run", "--rm", "--detach", "--publish="+hostPort+":5672", "--quiet", "--", "rabbitmq:4.1.1-alpine").Output()
		if err != nil {
			t.Fatalf("error launching rabbitmq in docker: %v", err)
		}
		id := strings.TrimSpace(string(out))
		t.Cleanup(func() {
			_ = exec.Command("docker", "rm", "--force", id).Run()
		})
		return id
	}

	brokerID := runBroker()
	conn := waitForHealthyAmqp(t, connStr, WithConnectionOptionsReconnectInterval(100*time.Millisecond))

	if err := exec.Command("docker", "rm", "--force", brokerID).Run(); err != nil {
		t.Fatal("failed to kill broker", err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for !conn.IsClosed() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for connection to notice dead broker")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// the reconnect loop is now dialing a dead broker
	_ = conn.Close()

	runBroker()
	healthy := waitForHealthyAmqp(t, connStr, WithConnectionOptionsReconnectInterval(100*time.Millisecond))
	defer healthy.Close()

	// give the closed connection's reconnect loop time to (wrongly) reconnect
	for range 20 {
		if !conn.IsClosed() {
			t.Fatal("closed connection reconnected to the restarted broker")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
