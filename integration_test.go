package rabbitmq

import (
	"context"
	"errors"
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

func TestCloseIsIdempotent(t *testing.T) {
	connStr := prepareDockerTest(t)
	conn := waitForHealthyAmqp(t, connStr)

	consumer, err := NewConsumer(conn, "close_is_idempotent")
	if err != nil {
		t.Fatal("error creating consumer", err)
	}
	consumer.Close()
	consumer.Close()
	consumer.CloseWithContext(context.Background())

	if err := conn.Close(); err != nil {
		t.Fatal("error closing connection", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal("second connection close returned an error", err)
	}
}

func TestPublisherRestoresConfirmModeBeforeReconnectCompletes(t *testing.T) {
	connStr := prepareDockerTest(t)
	conn := waitForHealthyAmqp(t, connStr, WithConnectionOptionsBaseReconnectInterval(10*time.Millisecond))
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
	conn := waitForHealthyAmqp(t, connStr, WithConnectionOptionsBaseReconnectInterval(100*time.Millisecond))

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
	healthy := waitForHealthyAmqp(t, connStr, WithConnectionOptionsBaseReconnectInterval(100*time.Millisecond))
	defer healthy.Close()

	// give the closed connection's reconnect loop time to (wrongly) reconnect
	for range 20 {
		if !conn.IsClosed() {
			t.Fatal("closed connection reconnected to the restarted broker")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestPublishWithOutcomeExactFailedSet pipelines a large mixed batch of
// routable and unroutable mandatory messages and asserts the failed set is
// reconstructed exactly, with each returned message paired with its own
// return (verified by body), regardless of confirmation interleaving.
// probeBody is the payload waitForRoutable publishes. Handlers in these tests
// skip it so it does not count towards delivery assertions.
const probeBody = "routability-probe"

// waitForRoutable blocks until a mandatory publish to exchange/routingKey is no
// longer returned as unroutable. A consumer declares its queue and bindings
// inside Run, which the tests start in a goroutine, so without this the first
// messages of a batch can be genuinely unroutable and the failed set is larger
// than the test intends.
func waitForRoutable(ctx context.Context, t *testing.T, publisher *Publisher, exchange, routingKey string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		batch, err := publisher.PublishWithOutcome(ctx, []byte(probeBody), []string{routingKey},
			WithPublishOptionsExchange(exchange),
			WithPublishOptionsMandatory,
		)
		if err == nil && len(batch) == 1 {
			outcome, waitErr := batch[0].Wait(ctx)
			if waitErr == nil && !outcome.Failed() {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("routing key %q on exchange %q never became routable", routingKey, exchange)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestPublishWithOutcomeExactFailedSet(t *testing.T) {
	const messageCount = 400

	connStr := prepareDockerTest(t)
	conn := waitForHealthyAmqp(t, connStr)
	defer conn.Close()

	queueName := "outcome_queue"
	consumer, err := NewConsumer(conn, queueName, WithConsumerOptionsLogger(simpleLogF(t.Logf)))
	if err != nil {
		t.Fatal("error creating consumer", err)
	}
	defer consumer.CloseWithContext(context.Background())
	go func() {
		_ = consumer.Run(func(d Delivery) Action { return Ack })
	}()

	publisher, err := NewPublisher(conn,
		WithPublisherOptionsLogger(simpleLogF(t.Logf)),
		WithPublisherOptionsConfirm,
		WithPublisherOptionsMaxOutcomesInFlight(64),
	)
	if err != nil {
		t.Fatal("error creating publisher", err)
	}
	defer publisher.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	waitForRoutable(ctx, t, publisher, "", queueName)

	// the ref carries what the assertions need, so the test keeps no parallel
	// index from outcomes back to messages
	type sent struct {
		index      int
		body       []byte
		unroutable bool
	}

	var outcomes []*PublishOutcome
	for i := range messageCount {
		msg := &sent{
			index:      i,
			body:       []byte(fmt.Sprintf("message-%d", i)),
			unroutable: i%5 == 0,
		}
		routingKey := queueName
		if msg.unroutable {
			routingKey = "no-such-queue"
		}
		batch, err := publisher.PublishWithOutcome(ctx, msg.body, []string{routingKey},
			WithPublishOptionsMandatory,
			WithPublishOptionsOutcomeRef(msg),
		)
		if err != nil {
			t.Fatalf("publish %d failed: %v", i, err)
		}
		outcomes = append(outcomes, batch...)
	}

	seenIDs := make(map[string]int, messageCount)
	for _, po := range outcomes {
		outcome, err := po.Wait(ctx)
		if err != nil {
			t.Fatalf("timed out waiting for outcome %s: %v", outcome.ID, err)
		}
		msg, ok := outcome.Ref.(*sent)
		if !ok {
			t.Fatalf("outcome %+v did not carry its ref", outcome)
		}
		if outcome.Err != nil {
			t.Fatalf("message %d: unexpected outcome error: %v", msg.index, outcome.Err)
		}
		if outcome.Failed() != msg.unroutable {
			t.Fatalf("message %d: Failed() = %v, want %v (outcome %+v)", msg.index, outcome.Failed(), msg.unroutable, outcome)
		}
		if outcome.ID == "" {
			t.Fatalf("message %d: outcome carries no ID", msg.index)
		}
		if prev, dup := seenIDs[outcome.ID]; dup {
			t.Fatalf("outcome ID %q reused by messages %d and %d", outcome.ID, prev, msg.index)
		}
		seenIDs[outcome.ID] = msg.index
		// published to the default exchange
		if outcome.Exchange != "" {
			t.Fatalf("message %d: Exchange = %q, want the default exchange", msg.index, outcome.Exchange)
		}
		if msg.unroutable {
			if !outcome.Ack || outcome.Return == nil {
				t.Fatalf("message %d: unroutable message should be acked with a return, got %+v", msg.index, outcome)
			}
			if got, want := string(outcome.Return.Body), string(msg.body); got != want {
				t.Fatalf("message %d: return mis-paired, body %q, want %q", msg.index, got, want)
			}
		}
	}
	if len(seenIDs) != messageCount {
		t.Fatalf("resolved %d outcomes, want %d", len(seenIDs), messageCount)
	}
}

// TestPublishWithOutcomeRepublishFailedSet is the acceptance test for acting on
// the failed set: the failed outcomes are collected as plain Outcome values,
// detached from the futures that produced them, and are still enough to
// republish every message. Note that the test keeps no map from outcomes back
// to messages - that is the point.
func TestPublishWithOutcomeRepublishFailedSet(t *testing.T) {
	const (
		messageCount = 120
		exchangeName = "outcome_retry_exchange"
	)

	connStr := prepareDockerTest(t)
	conn := waitForHealthyAmqp(t, connStr)
	defer conn.Close()

	queueName := "outcome_retry_queue"

	var mu sync.Mutex
	delivered := map[string]int{}
	consumer, err := NewConsumer(conn, queueName,
		WithConsumerOptionsLogger(simpleLogF(t.Logf)),
		WithConsumerOptionsExchangeName(exchangeName),
		WithConsumerOptionsExchangeDeclare,
		WithConsumerOptionsRoutingKey(queueName),
	)
	if err != nil {
		t.Fatal("error creating consumer", err)
	}
	defer consumer.CloseWithContext(context.Background())
	go func() {
		_ = consumer.Run(func(d Delivery) Action {
			if string(d.Body) != probeBody {
				mu.Lock()
				delivered[string(d.Body)]++
				mu.Unlock()
			}
			return Ack
		})
	}()

	publisher, err := NewPublisher(conn,
		WithPublisherOptionsLogger(simpleLogF(t.Logf)),
		WithPublisherOptionsConfirm,
		WithPublisherOptionsExchangeName(exchangeName),
		WithPublisherOptionsExchangeDeclare,
		WithPublisherOptionsMaxOutcomesInFlight(32),
	)
	if err != nil {
		t.Fatal("error creating publisher", err)
	}
	defer publisher.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	waitForRoutable(ctx, t, publisher, exchangeName, queueName)

	// half the batch is aimed at a routing key nothing is bound to
	wantRetried := map[string]bool{}
	wantRouted := map[string]bool{}
	var outcomes []*PublishOutcome
	for i := range messageCount {
		body := fmt.Sprintf("routed-%d", i)
		routingKey := queueName
		if i%2 == 0 {
			body = fmt.Sprintf("unrouted-%d", i)
			routingKey = "no-such-binding"
			wantRetried[body] = true
		} else {
			wantRouted[body] = true
		}
		batch, err := publisher.PublishWithOutcome(ctx, []byte(body), []string{routingKey},
			WithPublishOptionsExchange(exchangeName),
			WithPublishOptionsMandatory,
			WithPublishOptionsOutcomeRef([]byte(body)),
		)
		if err != nil {
			t.Fatalf("publish %d failed: %v", i, err)
		}
		outcomes = append(outcomes, batch...)
	}

	var failed []Outcome
	for _, po := range outcomes {
		outcome, err := po.Wait(ctx)
		if err != nil {
			t.Fatalf("timed out waiting for outcome %s: %v", outcome.ID, err)
		}
		if outcome.Failed() {
			failed = append(failed, outcome)
		}
	}
	if len(failed) != len(wantRetried) {
		t.Fatalf("failed set has %d outcomes, want %d", len(failed), len(wantRetried))
	}

	// republish from the outcomes alone: Exchange says where the message
	// belonged and Ref carries the payload back
	var retries []*PublishOutcome
	for _, outcome := range failed {
		body, ok := outcome.Ref.([]byte)
		if !ok {
			t.Fatalf("failed outcome %+v did not carry its ref", outcome)
		}
		if !wantRetried[string(body)] {
			t.Fatalf("unexpected message in the failed set: %q", body)
		}
		batch, err := publisher.PublishWithOutcome(ctx, body, []string{queueName},
			WithPublishOptionsExchange(outcome.Exchange),
			WithPublishOptionsMandatory,
		)
		if err != nil {
			t.Fatalf("republish of %q failed: %v", body, err)
		}
		retries = append(retries, batch...)
	}

	for _, po := range retries {
		outcome, err := po.Wait(ctx)
		if err != nil {
			t.Fatalf("timed out waiting for retry outcome %s: %v", outcome.ID, err)
		}
		if outcome.Failed() {
			t.Fatalf("republished message still failed: %+v", outcome)
		}
	}

	// every originally-routed message and every retried message arrives once
	deadline := time.Now().Add(20 * time.Second)
	for {
		mu.Lock()
		count := len(delivered)
		mu.Unlock()
		if count >= len(wantRouted)+len(wantRetried) || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	for body := range wantRetried {
		if delivered[body] != 1 {
			t.Errorf("retried message %q delivered %d times, want 1", body, delivered[body])
		}
	}
	for body := range wantRouted {
		if delivered[body] != 1 {
			t.Errorf("routed message %q delivered %d times, want 1", body, delivered[body])
		}
	}
	if len(delivered) != len(wantRouted)+len(wantRetried) {
		t.Errorf("consumer saw %d distinct bodies, want %d", len(delivered), len(wantRouted)+len(wantRetried))
	}
}

// TestPublishWithOutcomeNonMandatory documents that without the Mandatory
// option the broker silently drops unroutable messages and still confirms
// them, so the outcome cannot detect the routing failure.
func TestPublishWithOutcomeNonMandatory(t *testing.T) {
	connStr := prepareDockerTest(t)
	conn := waitForHealthyAmqp(t, connStr)
	defer conn.Close()

	publisher, err := NewPublisher(conn,
		WithPublisherOptionsLogger(simpleLogF(t.Logf)),
		WithPublisherOptionsConfirm,
	)
	if err != nil {
		t.Fatal("error creating publisher", err)
	}
	defer publisher.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := publisher.PublishWithOutcome(ctx, []byte("dropped"), []string{"no-such-queue"})
	if err != nil {
		t.Fatal("publish failed", err)
	}
	outcome, err := batch[0].Wait(ctx)
	if err != nil {
		t.Fatal("timed out waiting for outcome", err)
	}
	if outcome.Failed() || !outcome.Ack || outcome.Return != nil {
		t.Fatalf("non-mandatory unroutable message should be a clean ack, got %+v", outcome)
	}
}

// TestPublishWithOutcomeChannelReconnect forces a channel-level reconnect
// with a batch in flight: every outcome must still resolve (as an ack or as
// ErrOutcomeUnknown, never hang), and after the reconnect both confirmations
// and returns must keep flowing on the replacement channel.
func TestPublishWithOutcomeChannelReconnect(t *testing.T) {
	const messageCount = 100

	connStr := prepareDockerTest(t)
	conn := waitForHealthyAmqp(t, connStr, WithConnectionOptionsBaseReconnectInterval(10*time.Millisecond))
	defer conn.Close()

	queueName := "outcome_reconnect_queue"
	consumer, err := NewConsumer(conn, queueName, WithConsumerOptionsLogger(simpleLogF(t.Logf)))
	if err != nil {
		t.Fatal("error creating consumer", err)
	}
	defer consumer.CloseWithContext(context.Background())
	go func() {
		_ = consumer.Run(func(d Delivery) Action { return Ack })
	}()

	publisher, err := NewPublisher(conn,
		WithPublisherOptionsLogger(simpleLogF(t.Logf)),
		WithPublisherOptionsConfirm,
	)
	if err != nil {
		t.Fatal("error creating publisher", err)
	}
	defer publisher.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var outcomes []*PublishOutcome
	for i := range messageCount {
		batch, err := publisher.PublishWithOutcome(ctx, []byte("in-flight"), []string{queueName}, WithPublishOptionsMandatory)
		if err != nil {
			t.Fatalf("publish %d failed: %v", i, err)
		}
		outcomes = append(outcomes, batch...)
	}

	// force a channel-level close while confirmations are in flight
	reconnectionCount := publisher.chanManager.GetReconnectionCount()
	_ = publisher.Publish([]byte("boom"), []string{"unused"}, WithPublishOptionsExchange("missing-exchange"))
	deadline := time.Now().Add(10 * time.Second)
	for publisher.chanManager.GetReconnectionCount() == reconnectionCount {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for channel reconnect")
		}
		runtime.Gosched()
	}

	unknown := 0
	for i, po := range outcomes {
		outcome, err := po.Wait(ctx)
		if err != nil {
			t.Fatalf("message %d: outcome hung across reconnect: %v", i, err)
		}
		switch {
		case outcome.Err != nil:
			if !errors.Is(outcome.Err, ErrOutcomeUnknown) {
				t.Fatalf("message %d: unexpected error %v", i, outcome.Err)
			}
			unknown++
		case outcome.Ack && outcome.Return == nil:
			// confirmed before the channel died
		default:
			t.Fatalf("message %d: unexpected outcome %+v", i, outcome)
		}
	}
	t.Logf("%d of %d in-flight outcomes resolved as unknown across the reconnect", unknown, messageCount)

	// publishing gates on the returns listener covering the new channel, so
	// the very first post-reconnect unroutable publish must resolve with its
	// return; a clean ack here would be a silently false success
	batch, err := publisher.PublishWithOutcome(ctx, []byte("post-reconnect"), []string{"no-such-queue"}, WithPublishOptionsMandatory)
	if err != nil {
		t.Fatalf("publish after reconnect failed: %v", err)
	}
	outcome, err := batch[0].Wait(ctx)
	if err != nil {
		t.Fatal("timed out waiting for post-reconnect outcome", err)
	}
	if !outcome.Ack || outcome.Return == nil || outcome.Err != nil {
		t.Fatalf("post-reconnect unroutable message must carry its return, got %+v", outcome)
	}

	// and routable messages must confirm cleanly
	batch, err = publisher.PublishWithOutcome(ctx, []byte("routable"), []string{queueName}, WithPublishOptionsMandatory)
	if err != nil {
		t.Fatal("routable publish after reconnect failed", err)
	}
	outcome, err = batch[0].Wait(ctx)
	if err != nil {
		t.Fatal("timed out waiting for routable outcome", err)
	}
	if outcome.Failed() {
		t.Fatalf("routable message after reconnect should succeed, got %+v", outcome)
	}
}
