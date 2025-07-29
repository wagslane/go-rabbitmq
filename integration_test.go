//go:build integration

package rabbitmq

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func prepareDockerTest(t *testing.T) (connStr string) {
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

	// Set up a consumer which pushes each of its consumed messages over the channel. If the channel is closed or full
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

// TestNotifyPublishWithReturn tests the NotifyPublishWithReturn functionality by publishing both
// routable and unroutable messages and verifying the handler receives the correct confirmations and returns.
func TestNotifyPublishWithReturn(t *testing.T) {
	connStr := prepareDockerTest(t)
	conn := waitForHealthyAmqp(t, connStr)
	defer conn.Close()

	// Define a struct to hold the results of the publish operation
	type publishResult struct {
		confirmation Confirmation
		returnInfo   Return
		hasReturn    bool
	}

	// Helper function to set up publisher and result channel
	setupPublisher := func(t *testing.T) (*Publisher, chan publishResult) {
		publisher, err := NewPublisher(conn, WithPublisherOptionsLogger(simpleLogF(t.Logf)))
		if err != nil {
			t.Fatal("error creating publisher", err)
		}
		results := make(chan publishResult, 10)

		// Set up the handler
		publisher.NotifyPublishWithReturn(func(p Confirmation, r Return) {
			hasReturn := r.ReplyCode != 0
			t.Logf("NotifyPublishWithReturn called: ack=%v, hasReturn=%v, replyCode=%d",
				p.Ack, hasReturn, r.ReplyCode)

			results <- publishResult{
				confirmation: p,
				returnInfo:   r,
				hasReturn:    hasReturn,
			}
		})

		return publisher, results
	}

	t.Run("UnroutableMandatoryMessage_ShouldBeAckedAndReturned", func(t *testing.T) {
		publisher, results := setupPublisher(t)
		defer publisher.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		// Publish to non-existent queue with mandatory=true (should return)
		t.Logf("publishing to non-existent queue (should be returned)")
		err := publisher.PublishWithContext(
			ctx,
			[]byte("test message 1"),
			[]string{"non-existent-queue"},
			WithPublishOptionsMandatory,
		)
		if err != nil {
			t.Fatal("failed to publish to non-existent queue", err)
		}

		// Wait for the return + confirmation
		select {
		case result := <-results:
			if !result.hasReturn {
				t.Fatal("expected a return for unroutable message, but got none")
			}
			if result.returnInfo.ReplyCode == 0 {
				t.Fatal("expected non-zero reply code for returned message")
			}
			if !result.confirmation.Ack {
				t.Fatal("expected message to be acked, but it was nacked")
			}
			t.Logf("correctly received return: replyCode=%d, replyText=%s",
				result.returnInfo.ReplyCode, result.returnInfo.ReplyText)
		case <-time.After(time.Second * 5):
			t.Fatal("timeout waiting for return notification")
		}
	})

	t.Run("RoutableMandatoryMessage_ShouldBeAckedWithoutReturn", func(t *testing.T) {
		publisher, results := setupPublisher(t)
		defer publisher.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		// Create a queue and publish to it (should succeed, no return)
		consumerQueue := "test_queue_" + fmt.Sprintf("%d", time.Now().UnixNano())
		consumer, err := NewConsumer(conn, consumerQueue, WithConsumerOptionsLogger(simpleLogF(t.Logf)))
		if err != nil {
			t.Fatal("error creating consumer", err)
		}
		defer consumer.CloseWithContext(context.Background())

		// Start consumer to ensure queue exists
		consumed := make(chan Delivery, 1)
		go func() {
			err = consumer.Run(func(d Delivery) Action {
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

		// Wait for the consumer to be ready
		time.Sleep(100 * time.Millisecond)

		t.Logf("publishing to existing queue (should succeed)")
		err = publisher.PublishWithContext(
			ctx,
			[]byte("test message 2"),
			[]string{consumerQueue},
			WithPublishOptionsMandatory,
		)
		if err != nil {
			t.Fatal("failed to publish to existing queue", err)
		}

		// Wait for the confirmation (no return expected)
		select {
		case result := <-results:
			if result.hasReturn {
				t.Fatalf("unexpected return for routable message: replyCode=%d, replyText=%s",
					result.returnInfo.ReplyCode, result.returnInfo.ReplyText)
			}
			if !result.confirmation.Ack {
				t.Fatal("expected message to be acked, but it was nacked")
			}
			t.Logf("correctly received confirmation without return: ack=%v", result.confirmation.Ack)
		case <-time.After(time.Second * 5):
			t.Fatal("timeout waiting for confirmation notification")
		}

		// Verify the message was actually consumed
		select {
		case d := <-consumed:
			t.Logf("message successfully consumed: '%s'", string(d.Body))
			if string(d.Body) != "test message 2" {
				t.Fatalf("expected message 'test message 2', got '%s'", string(d.Body))
			}
		case <-time.After(time.Second * 3):
			t.Fatal("timeout waiting for message consumption")
		}
	})

	t.Run("UnroutableNonMandatoryMessage_ShouldBeAckedWithoutReturn", func(t *testing.T) {
		publisher, results := setupPublisher(t)
		defer publisher.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		// Publish without mandatory flag to non-existent queue (should succeed, no return)
		t.Logf("publishing to non-existent queue without mandatory (should succeed)")
		err := publisher.PublishWithContext(
			ctx,
			[]byte("test message 3"),
			[]string{"another-non-existent-queue"},
			// No WithPublishOptionsMandatory - defaults to false
		)
		if err != nil {
			t.Fatal("failed to publish without mandatory", err)
		}

		// Wait for the confirmation (no return expected)
		select {
		case result := <-results:
			if result.hasReturn {
				t.Fatalf("unexpected return for non-mandatory message: replyCode=%d, replyText=%s",
					result.returnInfo.ReplyCode, result.returnInfo.ReplyText)
			}
			if !result.confirmation.Ack {
				t.Fatal("expected non-mandatory message to be acked, but it was nacked")
			}
			t.Logf("correctly received confirmation without return for non-mandatory: ack=%v", result.confirmation.Ack)
		case <-time.After(time.Second * 5):
			t.Fatal("timeout waiting for non-mandatory confirmation notification")
		}
	})
}
