package rabbitmq

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
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

	out, err := exec.CommandContext(ctx, "docker", "run", "--rm", "--detach", "--publish=5672:5672", "--quiet", "--", "rabbitmq:3-alpine").Output()
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

func waitForHealthyAmqp(t *testing.T, connStr string) *Conn {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tkr := time.NewTicker(time.Second)

	// only log connection-level logs when connection has succeeded
	muted := true
	connLogger := simpleLogF(func(s string, i ...interface{}) {
		if !muted {
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
			conn, err := NewConn(connStr, WithConnectionOptionsLogger(connLogger))
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
					return pub.PublishWithContext(ctx, []byte{}, []string{"ping"}, WithPublishOptionsExchange(""))
				}(); err != nil {
					_ = conn.Close()
					t.Log("publish ping failed", err.Error())
				} else {
					t.Log("ping successful")
					muted = true
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
