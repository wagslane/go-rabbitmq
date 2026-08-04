package rabbitmq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

// outcomeChannelManager is the subset of the channel manager the outcome
// tracker relies on, extracted so the reconnect behavior can be tested
// without a broker.
//
// Contract: GetReconnectionCount advances together with the channel swap,
// while the channel lock is still held, so any operation that can reach the
// new channel also observes the new count. The tracker's listener gate and
// post-publish verification are unsound without this coupling — a publish
// could land on a freshly swapped channel (with no returns listener yet)
// while the count still reports the generation the listener covers.
type outcomeChannelManager interface {
	GetReconnectionCount() uint
	NotifyReturnSafe(c chan amqp.Return) chan amqp.Return
	NotifyReconnect() (<-chan error, chan<- struct{})
}

// OutcomeIDHeader is the message header PublishWithOutcome sets on every
// message to correlate broker returns with publisher confirmations. It is
// visible to consumers of the message, and any caller-set value for this
// header is overwritten.
const OutcomeIDHeader = "x-gorabbitmq-outcome-id"

// ErrOutcomeUnknown reports that the channel was lost around the time the
// message was in flight, so its fate could not be determined. The message may
// or may not have been delivered: republish it only if consumers tolerate
// duplicates (e.g. by deduplicating on a message ID).
//
// Classification is deliberately conservative: a genuine broker nack that
// races with a channel loss may occasionally be reported as unknown, but the
// reverse never happens — an outcome reported as a definite ack or nack was
// really sent by the broker on a live channel.
var ErrOutcomeUnknown = errors.New("outcome unknown: the channel was lost or replaced while the publishing was in flight")

// ErrPublisherClosed reports that the publisher was closed before the
// operation could complete.
var ErrPublisherClosed = errors.New("publisher is closed")

// bufferedReturnsCount is the buffer of the tracker's basic.return listener.
// Returns only flow for mandatory/immediate messages the broker cannot
// deliver; the buffer keeps the client's frame-dispatch goroutine from
// blocking during bursts of unroutable messages.
const bufferedReturnsCount = 64

// Outcome is the final fate of one message published with PublishWithOutcome.
//
// The fields fall into two groups. The identity group describes which message
// this is; it is filled in before the message is published and is never
// rewritten, so it is populated on every Outcome a caller can observe. The
// remaining fields are the message's fate, written once when the outcome
// resolves. Exchange and RoutingKey are what PublishWithOutcome needs to send
// the message again, so a failed Outcome says where it belongs without a side
// table from futures back to destinations.
type Outcome struct {
	// ID is the correlation identity assigned to this publishing and put on
	// the wire as the OutcomeIDHeader header, so a publisher outcome can be
	// matched against the consumer-side delivery. It is unique per publishing
	// and stable across reconnects, unlike DeliveryTag. It is not a logical
	// message identity: republishing a message assigns a new ID. For an
	// identity that survives a retry, set your own with
	// WithPublishOptionsMessageID.
	ID string
	// Exchange the message was published to. The empty string is the default
	// exchange.
	Exchange string
	// RoutingKey the message was published with.
	RoutingKey string
	// DeliveryTag the channel assigned to the publishing. Delivery tags are
	// per-channel and restart at 1 after a reconnect, so a tag identifies a
	// message only within one channel generation; use ID for an identity that
	// is stable across reconnects.
	DeliveryTag uint64
	// Ref is the value passed to WithPublishOptionsOutcomeRef, or nil. Every
	// outcome of one PublishWithOutcome call carries the same ref, so use
	// RoutingKey to tell them apart.
	Ref any
	// Ack is true when the broker took responsibility for the message. Note
	// that the broker acks mandatory messages it could not route (after
	// sending the return), so a routing failure is Ack=true with a non-nil
	// Return.
	Ack bool
	// Return is non-nil when the broker returned the message as unroutable.
	// The broker only issues returns for messages published with the
	// Mandatory (or Immediate) option; without it unroutable messages are
	// silently dropped and confirmed.
	Return *Return
	// Err is non-nil when no definite outcome could be determined, see
	// ErrOutcomeUnknown.
	Err error
}

// Failed reports whether the message needs to be republished: the broker
// nacked it, returned it as unroutable, or the outcome is unknown.
func (o Outcome) Failed() bool {
	return o.Err != nil || !o.Ack || o.Return != nil
}

// PublishOutcome resolves to the Outcome of one published message once the
// broker has confirmed or returned it, or the channel is lost.
type PublishOutcome struct {
	done chan struct{}
	// outcome is written in two disjoint halves. PublishWithOutcome fills in
	// the identity fields before the tracker goroutine is started and never
	// writes them again; the tracker writes only Ack, Return and Err, then
	// closes done. Because the halves are distinct fields, reading the
	// identity before done is closed does not race with the tracker.
	outcome Outcome
}

// Done returns a channel that is closed when the outcome is available.
func (p *PublishOutcome) Done() <-chan struct{} {
	return p.done
}

// Ref returns the value passed to WithPublishOptionsOutcomeRef, or nil. It is
// fixed when the message is published, so unlike the rest of the Outcome it is
// readable at any time, including while the outcome is still unresolved.
func (p *PublishOutcome) Ref() any {
	return p.outcome.Ref
}

// Wait blocks until the outcome is available or the context is done.
//
// On context expiry the returned Outcome still carries the message identity,
// with Err set to the context error, so the message can be identified without
// any caller-side bookkeeping. The message is genuinely still in flight in that
// case: like ErrOutcomeUnknown it may yet be delivered, so only republish it if
// consumers tolerate duplicates. Distinguish the two with errors.Is. The
// outcome keeps resolving in the background, and Done and a later Wait still
// report its true fate.
func (p *PublishOutcome) Wait(ctx context.Context) (Outcome, error) {
	select {
	case <-p.done:
		return p.outcome, nil
	case <-ctx.Done():
		// the context error goes on the returned copy only, never into
		// p.outcome, so a later Wait still sees the real outcome
		pending := p.identity()
		pending.Err = ctx.Err()
		return pending, ctx.Err()
	}
}

// identity is the half of the outcome that is safe to read before it resolves:
// these fields are written by PublishWithOutcome before the tracker goroutine
// starts and the tracker never writes them, so reading them does not race with
// a concurrent resolution. Do not widen this to Ack, Return or Err.
func (p *PublishOutcome) identity() Outcome {
	return Outcome{
		ID:          p.outcome.ID,
		Exchange:    p.outcome.Exchange,
		RoutingKey:  p.outcome.RoutingKey,
		DeliveryTag: p.outcome.DeliveryTag,
		Ref:         p.outcome.Ref,
	}
}

// deferredConfirmation is the subset of amqp.DeferredConfirmation the tracker
// relies on, extracted so the pairing logic can be tested without a broker.
type deferredConfirmation interface {
	Done() <-chan struct{}
	Acked() bool
}

type outcomeEntry struct {
	id string
	// gen is the tracker's channel generation observed when the message was
	// published. It is sampled before the publish on purpose: it can only be
	// less than or equal to the generation at any later event, so a channel
	// death after the publish always shows up as a mismatch. A confirmation
	// that resolves negatively under a different generation was synthesized
	// by the client library on channel close, not sent by the broker, and is
	// reported as ErrOutcomeUnknown.
	gen uint64
	// uncertain marks a publish that could not be verified to have gone out
	// on the same channel the returns listener is installed on. For such
	// messages the absence of a return proves nothing, so a bare ack resolves
	// as ErrOutcomeUnknown instead of success.
	uncertain bool
	dc        deferredConfirmation
	po        *PublishOutcome
}

// outcomeTracker pairs broker returns with publisher confirmations.
//
// All returns and all confirmation completions funnel through the single run
// goroutine. Because the amqp library delivers a message's basic.return into
// the returns listener strictly before it resolves that message's
// DeferredConfirmation, draining the returns listener before processing a
// completion guarantees the matching return has already been stashed. The
// stash is keyed by a per-publishing ID carried in a message header, so the
// pairing does not depend on any adjacency between returns and confirmations
// (confirmations are resequenced by delivery tag and may be collapsed by
// multiple-acks).
type outcomeTracker struct {
	chanManager   outcomeChannelManager
	logger        Logger
	publisherDone <-chan struct{}

	idPrefix string
	idSeq    atomic.Uint64

	// gen counts channel generations; bumped by the run goroutine each time
	// the returns listener closes (channel loss or publisher close).
	gen atomic.Uint64

	// listenerReconn is the reconnection count the returns listener is
	// currently installed for; listenerReady is closed and replaced whenever
	// it advances, waking publishes gated on listener currency.
	listenerMu     sync.Mutex
	listenerReconn uint
	listenerReady  chan struct{}

	// sem caps in-flight publishings; nil means unlimited.
	sem chan struct{}

	// completionCh must be unbuffered: a completion is either handed to the
	// run goroutine or, if it has exited, the sender observes exited and
	// resolves the entry itself. A buffered send could succeed after run
	// exits and strand the entry unresolved.
	completionCh chan *outcomeEntry
	outstanding  atomic.Int64
	// exited is closed when the run goroutine stops; completions that can no
	// longer be delivered resolve conservatively as ErrOutcomeUnknown.
	exited chan struct{}

	// returned stashes broker returns by outcome ID until the matching
	// confirmation arrives. Only the run goroutine touches it.
	returned map[string]*Return
}

func newOutcomeTracker(
	chanManager outcomeChannelManager,
	logger Logger,
	publisherDone <-chan struct{},
	maxInFlight int,
) *outcomeTracker {
	prefix := make([]byte, 8)
	if _, err := rand.Read(prefix); err != nil {
		// fall back to the address-derived uniqueness of the tracker itself;
		// IDs only need to be unique within one channel's lifetime
		prefix = []byte("gorabbit")
	}
	tracker := &outcomeTracker{
		chanManager:   chanManager,
		logger:        logger,
		publisherDone: publisherDone,
		idPrefix:      hex.EncodeToString(prefix) + "-",
		completionCh:  make(chan *outcomeEntry),
		exited:        make(chan struct{}),
		returned:      make(map[string]*Return),
		listenerReady: make(chan struct{}),
	}
	if maxInFlight > 0 {
		tracker.sem = make(chan struct{}, maxInFlight)
	}
	return tracker
}

// start registers the returns listener before the first publish can happen
// and launches the pairing goroutine.
func (t *outcomeTracker) start() {
	reconnCh, reconnStop := t.chanManager.NotifyReconnect()
	returns := t.registerReturnsListener()
	go t.run(returns, reconnCh, reconnStop)
}

// registerReturnsListener installs a returns listener on the current channel
// and records which reconnection generation it covers. The registration is
// sandwiched between two reads of the reconnection count: the count advances
// atomically with the channel swap, so when the reads agree the listener is
// installed on exactly that generation's channel — the count cannot lag a
// swap the registration observed. When the reads differ, it is unknown which
// channel the listener landed on; it cannot simply be dropped, because the amqp client
// has no listener deregistration and an unconsumed listener on the live
// channel would eventually fill and block the connection's frame dispatch.
// It is drained and discarded instead: discarding is safe because publishes
// are gated while no listener is confirmed current, so no outcome returns
// can arrive that the successful registration won't also receive.
func (t *outcomeTracker) registerReturnsListener() <-chan amqp.Return {
	for {
		before := t.chanManager.GetReconnectionCount()
		returns := t.chanManager.NotifyReturnSafe(make(chan amqp.Return, bufferedReturnsCount))
		if t.chanManager.GetReconnectionCount() != before {
			go discardReturns(returns)
			continue
		}
		t.listenerMu.Lock()
		t.listenerReconn = before
		close(t.listenerReady)
		t.listenerReady = make(chan struct{})
		t.listenerMu.Unlock()
		return returns
	}
}

// discardReturns keeps a superseded listener drained until its channel dies,
// so it can never block the connection's frame dispatch.
func discardReturns(returns <-chan amqp.Return) {
	for range returns {
	}
}

// listenerStale reports whether a reconnect has outpaced the recorded
// listener generation.
func (t *outcomeTracker) listenerStale() bool {
	t.listenerMu.Lock()
	installed := t.listenerReconn
	t.listenerMu.Unlock()
	return installed != t.chanManager.GetReconnectionCount()
}

// awaitCurrentListener blocks until the returns listener covers the current
// channel generation, so a publish cannot race ahead of the listener after a
// reconnect and have its basic.return silently dropped. It returns the
// covered generation for post-publish verification. In steady state this is
// two cheap reads.
func (t *outcomeTracker) awaitCurrentListener(ctx context.Context) (uint, error) {
	for {
		t.listenerMu.Lock()
		installed := t.listenerReconn
		ready := t.listenerReady
		t.listenerMu.Unlock()

		if installed == t.chanManager.GetReconnectionCount() {
			return installed, nil
		}
		select {
		case <-ready:
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-t.publisherDone:
			return 0, ErrPublisherClosed
		}
	}
}

func (t *outcomeTracker) run(returns <-chan amqp.Return, reconnCh <-chan error, reconnStop chan<- struct{}) {
	defer close(t.exited)
	defer close(reconnStop)

	done := t.publisherDone
	doneSeen := false

	for {
		// wait for a reconnect signal while there is no live returns
		// listener, or while the listener is stale — a reconnect completed
		// after the registration, so the pending signal should trigger
		// re-registration promptly rather than waiting for the superseded
		// listener's channel close to propagate
		var reconn <-chan error
		if (returns == nil || t.listenerStale()) && !doneSeen {
			reconn = reconnCh
		}

		select {
		case ret, ok := <-returns:
			if !ok {
				t.gen.Add(1)
				returns = nil
				continue
			}
			t.stashReturn(ret)

		case <-reconn:
			fresh := t.registerReturnsListener()
			if returns != nil {
				// the superseded listener may sit on the live channel:
				// capture what it received before the replacement was
				// registered (not yet duplicated anywhere), then keep it
				// drained — its future traffic also reaches the replacement
				if returns = t.drainReturns(returns); returns != nil {
					go discardReturns(returns)
				}
			}
			returns = fresh

		case entry := <-t.completionCh:
			returns = t.drainReturns(returns)
			t.resolve(entry)
			if doneSeen && t.outstanding.Load() == 0 {
				return
			}

		case <-done:
			doneSeen = true
			done = nil
			if t.outstanding.Load() == 0 {
				return
			}
		}
	}
}

// drainReturns consumes every immediately available return. It runs before a
// completion is processed: the return for the completing message (if any) is
// guaranteed to already be receivable, so after the drain the stash is
// authoritative for that message.
func (t *outcomeTracker) drainReturns(returns <-chan amqp.Return) <-chan amqp.Return {
	for returns != nil {
		select {
		case ret, ok := <-returns:
			if !ok {
				t.gen.Add(1)
				return nil
			}
			t.stashReturn(ret)
		default:
			return returns
		}
	}
	return nil
}

func (t *outcomeTracker) stashReturn(ret amqp.Return) {
	id, ok := ret.Headers[OutcomeIDHeader].(string)
	if !ok {
		// not published through PublishWithOutcome; the regular NotifyReturn
		// handler is responsible for it
		return
	}
	r := Return{ret}
	t.returned[id] = &r
}

func (t *outcomeTracker) resolve(entry *outcomeEntry) {
	po := entry.po
	po.outcome.Return = t.returned[entry.id]
	delete(t.returned, entry.id)

	if entry.dc.Acked() {
		po.outcome.Ack = true
		if entry.uncertain && po.outcome.Return == nil {
			// the publish could not be verified against the returns
			// listener's channel, so the missing return proves nothing; a
			// present return is positive evidence and stays definitive
			po.outcome.Err = ErrOutcomeUnknown
		}
	} else if entry.gen != t.gen.Load() {
		po.outcome.Err = ErrOutcomeUnknown
	}
	close(po.done)
	t.release()
	t.outstanding.Add(-1)
}

// await feeds the confirmation of one publishing to the run goroutine.
func (t *outcomeTracker) await(entry *outcomeEntry) {
	select {
	case <-entry.dc.Done():
	case <-t.exited:
		t.abandon(entry)
		return
	}
	select {
	case t.completionCh <- entry:
	case <-t.exited:
		t.abandon(entry)
	}
}

// abandon resolves an entry whose completion can no longer reach the run
// goroutine. Without the stash there is no way to tell whether the message
// was returned, so the outcome is conservatively unknown.
func (t *outcomeTracker) abandon(entry *outcomeEntry) {
	t.logger.Warnf("publisher closed with outcome still in flight, resolving %s as unknown", entry.id)
	entry.po.outcome.Err = ErrOutcomeUnknown
	close(entry.po.done)
	t.release()
	t.outstanding.Add(-1)
}

func (t *outcomeTracker) acquire(ctx context.Context) error {
	if t.sem == nil {
		return nil
	}
	select {
	case t.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-t.publisherDone:
		return ErrPublisherClosed
	}
}

func (t *outcomeTracker) release() {
	if t.sem != nil {
		<-t.sem
	}
}

func (t *outcomeTracker) nextID() string {
	return t.idPrefix + strconv.FormatUint(t.idSeq.Add(1), 10)
}

// PublishWithOutcome publishes the provided data to each routing key and
// returns one PublishOutcome per key: a future that resolves to the message's
// final fate once the broker confirms it, returns it as unroutable, or the
// channel is lost. Unlike consuming NotifyPublish and NotifyReturn as
// separate streams, the confirmation and the return of the same message are
// paired reliably, including with many messages in flight.
//
// Requirements and semantics:
//   - The publisher must be created with WithPublisherOptionsConfirm.
//   - Detecting unroutable messages requires WithPublishOptionsMandatory;
//     without it the broker silently drops and still acks unroutable messages.
//   - Every message is tagged with an OutcomeIDHeader header used for
//     correlation; consumers of the message can see it, and it is reported as
//     Outcome.ID so a publisher outcome can be matched against the delivery.
//   - Each Outcome carries the ID, Exchange and RoutingKey of its message, so
//     the failed subset of a batch says where each message belongs without a
//     side table. Use WithPublishOptionsOutcomeRef to attach your own value
//     and get it back as Outcome.Ref, rather than keeping a parallel index
//     from outcomes to the data they came from.
//   - When the channel is lost mid-flight, the outcome resolves with
//     ErrOutcomeUnknown: the message may or may not have been delivered, so
//     republishing can duplicate it.
//   - WithPublisherOptionsMaxOutcomesInFlight bounds memory by blocking
//     publishes until earlier outcomes resolve.
//   - The underlying client notifies all basic.return listeners sequentially
//     before processing further frames, so a slow handler registered with
//     NotifyReturn on the same publisher delays outcome resolution (it cannot
//     mis-pair outcomes, only delay them).
//
// The returned outcomes are in routing-key order, so on error the key whose
// publish failed is routingKeys[len(outcomes)] and the keys after it were never
// sent. Outcomes for routing keys already published are still returned
// alongside the error, since those messages were sent.
func (publisher *Publisher) PublishWithOutcome(
	ctx context.Context,
	data []byte,
	routingKeys []string,
	optionFuncs ...func(*PublishOptions),
) ([]*PublishOutcome, error) {
	if !publisher.options.ConfirmMode {
		return nil, errors.New("PublishWithOutcome requires confirm mode: create the publisher with WithPublisherOptionsConfirm")
	}
	if publisher.disablePublishDueToFlow.Load() {
		return nil, ErrPublishFlowPaused
	}
	if publisher.disablePublishDueToBlocked.Load() {
		return nil, ErrPublishBlocked
	}

	publisher.outcomesOnce.Do(publisher.startOutcomeTracker)
	tracker := publisher.outcomes

	options := buildPublishOptions(optionFuncs)

	var outcomes []*PublishOutcome
	for _, routingKey := range routingKeys {
		// wait until the returns listener covers the current channel; a
		// publish racing ahead of it after a reconnect would have its
		// basic.return delivered to no listener and report a false success
		listenerGen, err := tracker.awaitCurrentListener(ctx)
		if err != nil {
			return outcomes, err
		}
		if err := tracker.acquire(ctx); err != nil {
			return outcomes, err
		}

		id := tracker.nextID()
		// buildPublishing copies the caller's headers into a fresh table, so
		// adding the correlation header does not leak into caller state
		message := buildPublishing(options, data)
		message.Headers[OutcomeIDHeader] = id

		gen := tracker.gen.Load()
		conf, err := publisher.chanManager.PublishWithDeferredConfirmWithContextSafe(
			ctx,
			options.Exchange,
			routingKey,
			options.Mandatory,
			options.Immediate,
			message,
		)
		if err != nil {
			tracker.release()
			return outcomes, err
		}
		if conf == nil {
			tracker.release()
			return outcomes, errors.New("no deferred confirmation returned, channel is not in confirm mode")
		}

		// if a reconnect landed between the listener check and the publish,
		// the message may have gone out on a channel the listener doesn't
		// cover; resolve it conservatively rather than risk a false success.
		// This check is sound because the count advances before a swapped-in
		// channel becomes publishable: a publish that reached a newer channel
		// than the listener's is always visible as a count mismatch here
		uncertain := tracker.chanManager.GetReconnectionCount() != listenerGen

		po := &PublishOutcome{done: make(chan struct{})}
		// the identity half of the outcome is written before the tracker
		// goroutine exists, so it is readable while the outcome is unresolved
		// and is enough to place the message on a retry
		po.outcome.ID = id
		po.outcome.Exchange = options.Exchange
		po.outcome.RoutingKey = routingKey
		po.outcome.DeliveryTag = conf.DeliveryTag
		po.outcome.Ref = options.OutcomeRef
		tracker.outstanding.Add(1)
		go tracker.await(&outcomeEntry{id: id, gen: gen, uncertain: uncertain, dc: conf, po: po})
		outcomes = append(outcomes, po)
	}
	return outcomes, nil
}

func (publisher *Publisher) startOutcomeTracker() {
	publisher.outcomes = newOutcomeTracker(
		publisher.chanManager,
		publisher.options.Logger,
		publisher.done,
		publisher.options.MaxOutcomesInFlight,
	)
	publisher.outcomes.start()
}
