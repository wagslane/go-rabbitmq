package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type fakeDC struct {
	ack  bool
	done chan struct{}
}

func (f *fakeDC) Done() <-chan struct{} { return f.done }

func (f *fakeDC) Acked() bool {
	select {
	case <-f.done:
		return f.ack
	default:
		return false
	}
}

func resolvedDC(ack bool) *fakeDC {
	f := &fakeDC{ack: ack, done: make(chan struct{})}
	close(f.done)
	return f
}

// newTestTracker runs a tracker against test-controlled channels instead of a
// live amqp channel. Closing the returned stop func shuts the tracker down
// and waits for its goroutine to exit.
func newTestTracker(t *testing.T, maxInFlight int) (*outcomeTracker, chan amqp.Return, func()) {
	t.Helper()
	publisherDone := make(chan struct{})
	fake := &fakeChanManager{reconnCh: make(chan error, 1)}
	tracker := newOutcomeTracker(fake, stdDebugLogger{}, publisherDone, maxInFlight)
	returns := make(chan amqp.Return, bufferedReturnsCount)
	go tracker.run(returns, fake.reconnCh, make(chan struct{}, 1))
	return tracker, returns, func() {
		close(publisherDone)
		select {
		case <-tracker.exited:
		case <-time.After(5 * time.Second):
			t.Fatal("tracker did not exit")
		}
	}
}

// testRef stands in for a caller's own value attached with
// WithPublishOptionsOutcomeRef. It is a distinct type from the library's id so
// a test cannot pass by accidentally comparing the ref against Outcome.ID.
type testRef struct{ name string }

// submit registers one in-flight publishing with the tracker the same way
// PublishWithOutcome does, including the identity fields.
func submit(tracker *outcomeTracker, id string, dc deferredConfirmation) *PublishOutcome {
	po := &PublishOutcome{done: make(chan struct{})}
	po.outcome.ID = id
	po.outcome.Exchange = "test-exchange"
	po.outcome.RoutingKey = "test-key"
	po.outcome.Ref = &testRef{name: id}
	tracker.outstanding.Add(1)
	go tracker.await(&outcomeEntry{id: id, gen: tracker.gen.Load(), dc: dc, po: po})
	return po
}

func brokerReturn(id string) amqp.Return {
	return amqp.Return{
		ReplyCode:  312,
		ReplyText:  "NO_ROUTE",
		RoutingKey: "test-key",
		Headers:    amqp.Table{OutcomeIDHeader: id},
	}
}

func waitOutcome(t *testing.T, po *PublishOutcome) Outcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	outcome, err := po.Wait(ctx)
	if err != nil {
		t.Fatalf("timed out waiting for outcome: %v", err)
	}
	return outcome
}

// TestOutcomePairsReturnWithConfirmation loads both the return and the
// resolved confirmation before the tracker can observe either, proving the
// pairing does not depend on which channel the tracker's select happens to
// pick first: the drain of the returns listener before resolving a
// confirmation makes the stash authoritative.
func TestOutcomePairsReturnWithConfirmation(t *testing.T) {
	tracker, returns, stop := newTestTracker(t, 0)
	defer stop()

	returns <- brokerReturn(t.Name())
	returned := submit(tracker, t.Name(), resolvedDC(true))
	routed := submit(tracker, t.Name()+"-routed", resolvedDC(true))

	outcome := waitOutcome(t, returned)
	if !outcome.Ack || outcome.Return == nil || outcome.Err != nil {
		t.Fatalf("expected acked outcome with return, got %+v", outcome)
	}
	if !outcome.Failed() {
		t.Fatal("returned message must report Failed()")
	}
	if outcome.Return.ReplyCode != 312 {
		t.Fatalf("wrong return paired: %+v", outcome.Return)
	}

	outcome = waitOutcome(t, routed)
	if !outcome.Ack || outcome.Return != nil || outcome.Err != nil {
		t.Fatalf("expected clean ack, got %+v", outcome)
	}
	if outcome.Failed() {
		t.Fatal("acked unreturned message must not report Failed()")
	}
}

// TestOutcomeGenuineNack verifies a broker nack on a live channel is reported
// as a definite negative outcome, not an unknown one.
func TestOutcomeGenuineNack(t *testing.T) {
	tracker, _, stop := newTestTracker(t, 0)
	defer stop()

	outcome := waitOutcome(t, submit(tracker, t.Name(), resolvedDC(false)))
	if outcome.Ack || outcome.Err != nil {
		t.Fatalf("expected definite nack, got %+v", outcome)
	}
	if !outcome.Failed() {
		t.Fatal("nacked message must report Failed()")
	}
}

// TestOutcomeChannelLossResolvesUnknown simulates the amqp library's shutdown
// sequence: the returns listener closes, then pending confirmations are
// nacked. Those synthesized nacks must resolve as ErrOutcomeUnknown, not as
// definite broker nacks.
func TestOutcomeChannelLossResolvesUnknown(t *testing.T) {
	tracker, returns, stop := newTestTracker(t, 0)
	defer stop()

	dc := &fakeDC{ack: false, done: make(chan struct{})}
	po := submit(tracker, t.Name(), dc)

	close(returns)
	close(dc.done)

	outcome := waitOutcome(t, po)
	if !errors.Is(outcome.Err, ErrOutcomeUnknown) {
		t.Fatalf("expected ErrOutcomeUnknown, got %+v", outcome)
	}
	if !outcome.Failed() {
		t.Fatal("unknown outcome must report Failed()")
	}
}

// TestOutcomeReturnSurvivesChannelLoss: when the return arrived but the
// channel died before the confirmation, the outcome is unknown but still
// carries the return as evidence the message was unroutable.
func TestOutcomeReturnSurvivesChannelLoss(t *testing.T) {
	tracker, returns, stop := newTestTracker(t, 0)
	defer stop()

	dc := &fakeDC{ack: false, done: make(chan struct{})}
	po := submit(tracker, t.Name(), dc)

	returns <- brokerReturn(t.Name())
	close(returns)
	close(dc.done)

	outcome := waitOutcome(t, po)
	if !errors.Is(outcome.Err, ErrOutcomeUnknown) || outcome.Return == nil {
		t.Fatalf("expected unknown outcome with return attached, got %+v", outcome)
	}
}

// TestOutcomeRefStaysOffTheWire: the ref is caller-side state, so unlike
// CorrelationID it must not reach the broker in any form. buildPublishing
// enumerates AMQP fields explicitly, and this pins that down so adding a
// pass-through there cannot leak a caller's struct onto the wire.
func TestOutcomeRefStaysOffTheWire(t *testing.T) {
	secret := &testRef{name: "must-not-be-published"}
	options := buildPublishOptions([]func(*PublishOptions){
		WithPublishOptionsOutcomeRef(secret),
		WithPublishOptionsHeaders(Table{"keep": "me"}),
	})
	if options.OutcomeRef != secret {
		t.Fatal("WithPublishOptionsOutcomeRef did not set OutcomeRef")
	}

	message := buildPublishing(options, []byte("body"))
	for key, value := range message.Headers {
		if value == secret || value == any(secret) {
			t.Errorf("header %q carries the outcome ref", key)
		}
	}
	if _, ok := message.Headers["OutcomeRef"]; ok {
		t.Error("OutcomeRef leaked into the message headers")
	}
	if message.CorrelationId != "" || message.MessageId != "" || message.AppId != "" {
		t.Errorf("ref leaked into an AMQP property: %+v", message)
	}
	if got := message.Headers["keep"]; got != "me" {
		t.Errorf("caller headers were not preserved: %v", got)
	}
}

// TestOutcomeIdentitySurvivesEveryResolution asserts the identity half of an
// Outcome is intact no matter which path resolved it. The tracker writes only
// Ack, Return and Err, and this is the guard against a future rewrite that
// assigns po.outcome wholesale and silently drops the caller's ref.
func TestOutcomeIdentitySurvivesEveryResolution(t *testing.T) {
	for _, test := range []struct {
		name    string
		resolve func(t *testing.T, tracker *outcomeTracker, returns chan amqp.Return, id string) *PublishOutcome
	}{
		{"acked", func(_ *testing.T, tracker *outcomeTracker, _ chan amqp.Return, id string) *PublishOutcome {
			return submit(tracker, id, resolvedDC(true))
		}},
		{"returned and acked", func(_ *testing.T, tracker *outcomeTracker, returns chan amqp.Return, id string) *PublishOutcome {
			returns <- brokerReturn(id)
			return submit(tracker, id, resolvedDC(true))
		}},
		{"genuine nack", func(_ *testing.T, tracker *outcomeTracker, _ chan amqp.Return, id string) *PublishOutcome {
			return submit(tracker, id, resolvedDC(false))
		}},
		{"channel loss", func(_ *testing.T, tracker *outcomeTracker, returns chan amqp.Return, id string) *PublishOutcome {
			dc := &fakeDC{ack: false, done: make(chan struct{})}
			po := submit(tracker, id, dc)
			close(returns)
			close(dc.done)
			return po
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			tracker, returns, stop := newTestTracker(t, 0)
			defer stop()

			id := t.Name()
			outcome := waitOutcome(t, test.resolve(t, tracker, returns, id))
			assertIdentity(t, outcome, id)
		})
	}

	// abandon resolves entries whose completion can no longer reach the run
	// goroutine, and it is the one path that does not go through resolve
	t.Run("abandoned after tracker exit", func(t *testing.T) {
		tracker, _, stop := newTestTracker(t, 0)
		stop()

		id := t.Name()
		outcome := waitOutcome(t, submit(tracker, id, resolvedDC(true)))
		assertIdentity(t, outcome, id)
	})
}

func assertIdentity(t *testing.T, outcome Outcome, id string) {
	t.Helper()
	if outcome.ID != id {
		t.Errorf("ID = %q, want %q", outcome.ID, id)
	}
	if outcome.Exchange != "test-exchange" {
		t.Errorf("Exchange = %q, want %q", outcome.Exchange, "test-exchange")
	}
	if outcome.RoutingKey != "test-key" {
		t.Errorf("RoutingKey = %q, want %q", outcome.RoutingKey, "test-key")
	}
	ref, ok := outcome.Ref.(*testRef)
	if !ok {
		t.Fatalf("Ref = %#v, want a *testRef", outcome.Ref)
	}
	if ref.name != id {
		t.Errorf("Ref.name = %q, want %q", ref.name, id)
	}
}

// TestOutcomeWaitTimeoutKeepsIdentity: giving up on an unresolved outcome is
// exactly when a caller needs to know which message it gave up on, so the
// timeout path returns the identity rather than a zero Outcome. The context
// error must not be written into the outcome itself: a later Wait still has to
// report the message's real fate.
func TestOutcomeWaitTimeoutKeepsIdentity(t *testing.T) {
	tracker, _, stop := newTestTracker(t, 0)
	defer stop()

	id := t.Name()
	dc := &fakeDC{ack: true, done: make(chan struct{})}
	po := submit(tracker, id, dc)

	expired, cancel := context.WithCancel(context.Background())
	cancel()

	outcome, err := po.Wait(expired)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if !errors.Is(outcome.Err, context.Canceled) {
		t.Errorf("outcome.Err = %v, want context.Canceled", outcome.Err)
	}
	if !outcome.Failed() {
		t.Error("an outcome still in flight must report Failed()")
	}
	if outcome.Ack || outcome.Return != nil {
		t.Errorf("timeout must not report a fate, got %+v", outcome)
	}
	assertIdentity(t, outcome, id)

	// the real outcome is unaffected by having been waited on and abandoned
	close(dc.done)
	resolved := waitOutcome(t, po)
	if !resolved.Ack || resolved.Err != nil {
		t.Fatalf("resolved outcome = %+v, want acked with no error", resolved)
	}
	assertIdentity(t, resolved, id)
}

// TestOutcomeRefReadableWhileUnresolved is the executable form of the
// disjointness argument: the ref is written before the tracker goroutine
// starts and the tracker never touches it, so reading it concurrently with
// resolution is not a race. Only meaningful under -race.
func TestOutcomeRefReadableWhileUnresolved(t *testing.T) {
	tracker, _, stop := newTestTracker(t, 0)
	defer stop()

	const messages = 50
	var wg sync.WaitGroup
	for i := range messages {
		id := fmt.Sprintf("%s-%d", t.Name(), i)
		dc := &fakeDC{ack: true, done: make(chan struct{})}
		po := submit(tracker, id, dc)

		wg.Add(2)
		go func() {
			defer wg.Done()
			// read the ref while the tracker is resolving the same outcome
			for range 100 {
				if ref := po.Ref().(*testRef); ref.name != id {
					t.Errorf("Ref.name = %q, want %q", ref.name, id)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			close(dc.done)
		}()
	}
	wg.Wait()
}

func TestOutcomeFailed(t *testing.T) {
	ret := &Return{}
	for _, test := range []struct {
		name    string
		outcome Outcome
		failed  bool
	}{
		{"acked", Outcome{Ack: true}, false},
		{"nacked", Outcome{Ack: false}, true},
		{"acked but returned", Outcome{Ack: true, Return: ret}, true},
		{"unknown", Outcome{Err: ErrOutcomeUnknown}, true},
		{"still in flight", Outcome{Err: context.DeadlineExceeded}, true},
	} {
		if got := test.outcome.Failed(); got != test.failed {
			t.Errorf("%s: Failed() = %v, want %v", test.name, got, test.failed)
		}
	}
}

func TestOutcomeAcquireLimitAndCancellation(t *testing.T) {
	publisherDone := make(chan struct{})
	tracker := newOutcomeTracker(nil, stdDebugLogger{}, publisherDone, 1)

	if err := tracker.acquire(context.Background()); err != nil {
		t.Fatalf("first acquire should succeed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := tracker.acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline error while at capacity, got %v", err)
	}

	tracker.release()
	if err := tracker.acquire(context.Background()); err != nil {
		t.Fatalf("acquire after release should succeed: %v", err)
	}

	close(publisherDone)
	if err := tracker.acquire(context.Background()); !errors.Is(err, ErrPublisherClosed) {
		t.Fatalf("expected ErrPublisherClosed, got %v", err)
	}
}

// TestOutcomeAfterTrackerExitResolvesUnknown pins down the close race: a
// completion that can no longer reach the exited run goroutine must resolve
// as unknown instead of stranding the waiter. This relies on completionCh
// being unbuffered; with a buffer the send would succeed with no receiver
// and the outcome would hang forever.
func TestOutcomeAfterTrackerExitResolvesUnknown(t *testing.T) {
	tracker, _, stop := newTestTracker(t, 0)
	stop() // tracker has fully exited

	outcome := waitOutcome(t, submit(tracker, t.Name(), resolvedDC(true)))
	if !errors.Is(outcome.Err, ErrOutcomeUnknown) {
		t.Fatalf("expected ErrOutcomeUnknown after tracker exit, got %+v", outcome)
	}
}

// TestOutcomeCloseWithInFlight closes the publisher while confirmations are
// still pending: the tracker must keep running until every outstanding
// outcome resolves, then exit.
func TestOutcomeCloseWithInFlight(t *testing.T) {
	const inFlight = 20

	publisherDone := make(chan struct{})
	fake := &fakeChanManager{reconnCh: make(chan error, 1)}
	tracker := newOutcomeTracker(fake, stdDebugLogger{}, publisherDone, 0)
	returns := make(chan amqp.Return, bufferedReturnsCount)
	go tracker.run(returns, fake.reconnCh, make(chan struct{}, 1))

	var dcs []*fakeDC
	var outcomes []*PublishOutcome
	for i := range inFlight {
		dc := &fakeDC{ack: false, done: make(chan struct{})}
		dcs = append(dcs, dc)
		outcomes = append(outcomes, submit(tracker, fmt.Sprintf("%s-%d", t.Name(), i), dc))
	}

	// publisher closes first, then the amqp library closes the returns
	// listener and nacks the pending confirmations
	close(publisherDone)
	close(returns)
	for _, dc := range dcs {
		close(dc.done)
	}

	for i, po := range outcomes {
		outcome := waitOutcome(t, po)
		if !errors.Is(outcome.Err, ErrOutcomeUnknown) {
			t.Fatalf("outcome %d: expected ErrOutcomeUnknown, got %+v", i, outcome)
		}
	}
	select {
	case <-tracker.exited:
	case <-time.After(5 * time.Second):
		t.Fatal("tracker did not exit after all outcomes resolved")
	}
}

// fakeChanManager simulates the channel manager's reconnect surface so the
// listener-currency gate can be tested without a broker.
type fakeChanManager struct {
	count     atomic.Uint64
	mu        sync.Mutex
	listeners []chan amqp.Return
	reconnCh  chan error
	// onNotifyReturn, when set, runs during registration so tests can
	// simulate a reconnect landing mid-registration.
	onNotifyReturn func()
}

func (f *fakeChanManager) GetReconnectionCount() uint { return uint(f.count.Load()) }

func (f *fakeChanManager) NotifyReturnSafe(c chan amqp.Return) chan amqp.Return {
	f.mu.Lock()
	f.listeners = append(f.listeners, c)
	hook := f.onNotifyReturn
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	return c
}

func (f *fakeChanManager) NotifyReconnect() (<-chan error, chan<- struct{}) {
	return f.reconnCh, make(chan struct{}, 1)
}

func (f *fakeChanManager) listener(i int) chan amqp.Return {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listeners[i]
}

func (f *fakeChanManager) listenerCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.listeners)
}

// TestOutcomeListenerGateBlocksAcrossReconnect verifies publishes cannot race
// ahead of the returns listener after a reconnect: awaitCurrentListener must
// block from the moment the reconnection count advances until the tracker has
// re-registered its listener on the new channel.
func TestOutcomeListenerGateBlocksAcrossReconnect(t *testing.T) {
	fake := &fakeChanManager{reconnCh: make(chan error, 1)}
	publisherDone := make(chan struct{})
	tracker := newOutcomeTracker(fake, stdDebugLogger{}, publisherDone, 0)
	tracker.start()
	defer func() {
		close(publisherDone)
		select {
		case <-tracker.exited:
		case <-time.After(5 * time.Second):
			t.Fatal("tracker did not exit")
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// steady state: listener covers generation 0, no blocking
	gen, err := tracker.awaitCurrentListener(ctx)
	if err != nil || gen != 0 {
		t.Fatalf("expected immediate pass at generation 0, got %d, %v", gen, err)
	}

	// broker connection swaps: count advances, old listener closes
	fake.count.Store(1)
	close(fake.listener(0))

	gateResult := make(chan uint, 1)
	go func() {
		gen, err := tracker.awaitCurrentListener(ctx)
		if err != nil {
			t.Errorf("gate failed: %v", err)
		}
		gateResult <- gen
	}()

	select {
	case gen := <-gateResult:
		t.Fatalf("gate passed at generation %d before the listener was re-registered", gen)
	case <-time.After(100 * time.Millisecond):
	}

	// reconnect signal arrives: tracker re-registers and the gate opens
	fake.reconnCh <- errors.New("reconnected")
	select {
	case gen := <-gateResult:
		if gen != 1 {
			t.Fatalf("gate passed for generation %d, want 1", gen)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("gate never opened after listener re-registration")
	}
	if got := fake.listenerCount(); got != 2 {
		t.Fatalf("expected a second listener registration, got %d", got)
	}
}

// TestOutcomeMismatchedRegistrationIsDrained simulates a reconnect landing in
// the middle of listener registration: the superseded listener cannot be
// deregistered from the amqp client, so it must be kept drained (a full,
// unconsumed listener would block the connection's frame dispatch), and the
// retry must land on the new generation.
func TestOutcomeMismatchedRegistrationIsDrained(t *testing.T) {
	fake := &fakeChanManager{reconnCh: make(chan error, 1)}
	var once sync.Once
	fake.onNotifyReturn = func() {
		once.Do(func() { fake.count.Store(1) })
	}
	publisherDone := make(chan struct{})
	tracker := newOutcomeTracker(fake, stdDebugLogger{}, publisherDone, 0)
	tracker.start()
	defer func() {
		close(publisherDone)
		select {
		case <-tracker.exited:
		case <-time.After(5 * time.Second):
			t.Fatal("tracker did not exit")
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gen, err := tracker.awaitCurrentListener(ctx)
	if err != nil || gen != 1 {
		t.Fatalf("expected retried registration at generation 1, got %d, %v", gen, err)
	}
	if got := fake.listenerCount(); got != 2 {
		t.Fatalf("expected mismatched + retried registrations, got %d", got)
	}

	// overfill the superseded listener's buffer: if it were not drained,
	// these sends (like the client's dispatch goroutine) would block forever
	zombie := fake.listener(0)
	defer close(zombie)
	for i := 0; i < 2*bufferedReturnsCount; i++ {
		select {
		case zombie <- amqp.Return{}:
		case <-time.After(5 * time.Second):
			t.Fatal("superseded listener is not being drained")
		}
	}
}

// TestOutcomeUncertainPublishResolvesConservatively covers the residual race
// where a reconnect lands between the listener check and the publish: a bare
// ack proves nothing (the return may have gone to no listener) and must
// resolve as unknown, while a present return stays definitive.
func TestOutcomeUncertainPublishResolvesConservatively(t *testing.T) {
	tracker, returns, stop := newTestTracker(t, 0)
	defer stop()

	submitUncertain := func(id string) *PublishOutcome {
		po := &PublishOutcome{done: make(chan struct{})}
		po.outcome.ID = id
		po.outcome.Exchange = "test-exchange"
		po.outcome.RoutingKey = "test-key"
		po.outcome.Ref = &testRef{name: id}
		tracker.outstanding.Add(1)
		go tracker.await(&outcomeEntry{
			id: id, gen: tracker.gen.Load(), uncertain: true, dc: resolvedDC(true), po: po,
		})
		return po
	}

	outcome := waitOutcome(t, submitUncertain(t.Name()+"-bare-ack"))
	if !outcome.Ack || !errors.Is(outcome.Err, ErrOutcomeUnknown) {
		t.Fatalf("uncertain bare ack must resolve unknown, got %+v", outcome)
	}

	returns <- brokerReturn(t.Name() + "-returned")
	outcome = waitOutcome(t, submitUncertain(t.Name()+"-returned"))
	if !outcome.Ack || outcome.Return == nil || outcome.Err != nil {
		t.Fatalf("uncertain ack with return is definitive, got %+v", outcome)
	}
}

// TestOutcomeManyInFlight drives many concurrent publishings with a small
// in-flight cap and randomized return/confirm interleaving, asserting the
// failed set is reconstructed exactly. Run with -race.
func TestOutcomeManyInFlight(t *testing.T) {
	const messageCount = 500

	tracker, returns, stop := newTestTracker(t, 8)
	defer stop()

	wantFailed := map[string]bool{}
	outcomes := map[string]*PublishOutcome{}

	var wg sync.WaitGroup
	for i := range messageCount {
		id := fmt.Sprintf("msg-%d", i)
		unroutable := i%7 == 0
		wantFailed[id] = unroutable

		if err := tracker.acquire(context.Background()); err != nil {
			t.Fatalf("acquire: %v", err)
		}
		dc := &fakeDC{ack: true, done: make(chan struct{})}
		outcomes[id] = submit(tracker, id, dc)

		wg.Add(1)
		go func(id string, unroutable bool, dc *fakeDC) {
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
			if unroutable {
				// the broker sends the return strictly before the confirmation
				returns <- brokerReturn(id)
			}
			close(dc.done)
		}(id, unroutable, dc)
	}
	wg.Wait()

	for id, po := range outcomes {
		outcome := waitOutcome(t, po)
		if outcome.Err != nil {
			t.Fatalf("%s: unexpected error %v", id, outcome.Err)
		}
		if got, want := outcome.Failed(), wantFailed[id]; got != want {
			t.Fatalf("%s: Failed() = %v, want %v (outcome %+v)", id, got, want, outcome)
		}
	}
}
