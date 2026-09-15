package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/wagslane/go-rabbitmq/internal/channelmanager"
)

// Action is an action that occurs after processed this delivery
type Action int

// Handler defines the handler of each Delivery and return Action
type Handler func(d Delivery) (action Action)

const (
	// Ack default ack this msg after you have successfully processed this delivery.
	Ack Action = iota
	// NackDiscard the message will be dropped or delivered to a server configured dead-letter queue.
	NackDiscard
	// NackRequeue deliver this message to a different consumer.
	NackRequeue
	// Message acknowledgement is left to the user using the msg.Ack() method
	Manual
)

// Consumer allows you to create and connect to queues for data consumption.
type Consumer struct {
	chanManager                *channelmanager.ChannelManager
	reconnectErrCh             <-chan error
	closeConnectionToManagerCh chan<- struct{}
	options                    ConsumerOptions
	handlerMu                  *sync.RWMutex
	closeOnce                  sync.Once
	startupDone                chan struct{}
	startupOnce                sync.Once
	startupErr                 error

	isClosedMu *sync.RWMutex
	isClosed   bool
}

// Delivery captures the fields for a previously delivered message resident in
// a queue to be delivered by the server to a consumer from Channel.Consume or
// Channel.Get.
type Delivery struct {
	amqp.Delivery
}

// NewConsumer returns a new Consumer connected to the given rabbitmq server.
func NewConsumer(
	conn *Conn,
	queue string,
	optionFuncs ...func(*ConsumerOptions),
) (*Consumer, error) {
	defaultOptions := getDefaultConsumerOptions(queue)
	options := &defaultOptions
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	if conn.connectionManager == nil {
		return nil, errors.New("connection manager can't be nil")
	}

	chanManager, err := channelmanager.NewChannelManager(conn.connectionManager, options.Logger, conn.connectionManager.BaseReconnectInterval)
	if err != nil {
		return nil, err
	}
	reconnectErrCh, closeCh := chanManager.NotifyReconnect()

	consumer := &Consumer{
		chanManager:                chanManager,
		reconnectErrCh:             reconnectErrCh,
		closeConnectionToManagerCh: closeCh,
		options:                    *options,
		handlerMu:                  &sync.RWMutex{},
		isClosedMu:                 &sync.RWMutex{},
		isClosed:                   false,
		startupDone:                make(chan struct{}),
	}

	return consumer, nil
}

// Run starts consuming with automatic reconnection handling. Call Run only once.
// Use WaitReady to wait for initial setup separately from Run's lifetime.
// Run blocks until the consumer is closed. Recovery errors after the initial
// startup (including permanent ones like mismatched queue args) are logged
// and retried on the next reconnect rather than returned.
func (consumer *Consumer) Run(handler Handler) error {
	handlerWrapper := func(d Delivery) (action Action) {
		if !consumer.handlerMu.TryRLock() {
			// Leave the delivery unacked until channel close requeues it.
			// Nacking here can cause a hot redelivery loop during shutdown.
			return Manual
		}
		defer consumer.handlerMu.RUnlock()
		if consumer.getIsClosed() {
			return Manual
		}
		return handler(d)
	}

	err := consumer.startGoroutines(
		handlerWrapper,
		consumer.options,
	)
	consumer.finishStartup(err)
	if err != nil {
		return err
	}

	consumer.restartOnReconnect(func() error {
		// If the broker crashes again between reconnect and the
		// queue/exchange/binding declaration, startGoroutines returns
		// an error. Don't bail out of the loop - the underlying channel
		// manager will keep reconnecting and signal us again. Returning
		// here used to permanently stop the consumer.
		return consumer.startGoroutines(
			handlerWrapper,
			consumer.options,
		)
	})

	return nil
}

// WaitReady waits for Run's initial setup to finish. It returns nil after this
// consumer's QoS, declarations, bindings and basic.consume have succeeded, or
// returns the initial setup error. Run must be called separately, usually in a
// goroutine. WaitReady is safe to call before Run and from multiple goroutines.
//
// WaitReady reports initial readiness only, not current connection health. Its
// result is retained across reconnects. Canceling ctx stops only the wait; it
// does not stop Run or close the consumer. Closing before initial readiness
// returns amqp.ErrClosed.
//
// WithConsumerOptionsConsumerNoWait is incompatible with confirmed readiness:
// WaitReady returns an error because basic.consume-ok is not requested. Run's
// behavior with that option is unchanged.
func (consumer *Consumer) WaitReady(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if consumer.options.RabbitConsumerOptions.NoWait {
		return errors.New("consumer readiness requires RabbitConsumerOptions.NoWait=false")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-consumer.startupDone:
		return consumer.startupErr
	}
}

func (consumer *Consumer) finishStartup(err error) {
	consumer.startupOnce.Do(func() {
		consumer.startupErr = err
		close(consumer.startupDone)
	})
}

func (consumer *Consumer) restartOnReconnect(restart func() error) {
	for reconnectErr := range consumer.reconnectErrCh {
		consumer.options.Logger.Infof("successful consumer recovery from: %v", reconnectErr)
		if err := restart(); err != nil {
			consumer.options.Logger.Warnf(
				"error restarting consumer goroutines after reconnect, waiting for next reconnect: %v", err,
			)
		}
	}
}

// Close cleans up resources and closes the consumer.
// It waits for handler to finish before returning by default
// (use WithConsumerOptionsForceShutdown option to disable this behavior).
// Use CloseWithContext to specify a context to cancel the handler completion.
// It does not close the connection manager, just the subscription
// to the connection manager and the consuming goroutines.
func (consumer *Consumer) Close() {
	consumer.close(context.Background(), consumer.options.CloseGracefully, nil)
}

// CloseWithDrain stops admitting handler calls, waits for active handlers, then
// calls drain before closing the AMQP channel. Use it when handlers hand Manual
// deliveries to a local buffer: drain can flush that buffer and Ack or Nack its
// deliveries while the channel is still open. Network failures can still make
// acknowledgements fail; this is not a delivery guarantee.
//
// Deliveries not admitted to a handler remain unacked and are requeued when the
// channel closes. This avoids a Nack/requeue loop while drain runs. Configure a
// finite QOSPrefetch to bound those deliveries.
//
// Unlike Close with WithConsumerOptionsForceShutdown, CloseWithDrain always
// waits for active handlers. The callback runs synchronously and may be nil.
// Close methods share one shutdown: only the first call's callback can run.
func (consumer *Consumer) CloseWithDrain(drain func()) {
	consumer.close(context.Background(), true, drain)
}

func (consumer *Consumer) close(ctx context.Context, wait bool, drain func()) {
	consumer.closeOnce.Do(func() {
		consumer.isClosedMu.Lock()
		consumer.isClosed = true
		consumer.isClosedMu.Unlock()
		consumer.finishStartup(amqp.ErrClosed)
		defer consumer.cleanupResources()
		if wait {
			consumer.options.Logger.Infof("waiting for handler to finish...")
			if err := consumer.waitForHandlerCompletion(ctx); err != nil {
				consumer.options.Logger.Warnf("error while waiting for handler to finish: %v", err)
			}
		}
		if drain != nil {
			drain()
		}
	})
}

func (consumer *Consumer) cleanupResources() {
	consumer.isClosedMu.Lock()
	defer consumer.isClosedMu.Unlock()
	consumer.isClosed = true
	// close the channel so that rabbitmq server knows that the
	// consumer has been stopped.
	err := consumer.chanManager.Close()
	if err != nil {
		consumer.options.Logger.Warnf("error while closing the channel: %v", err)
	}

	consumer.options.Logger.Infof("closing consumer...")
	go func() {
		consumer.closeConnectionToManagerCh <- struct{}{}
	}()
}

// CloseWithContext cleans up resources and closes the consumer.
// It waits for handler to finish before returning
// (use WithConsumerOptionsForceShutdown option to disable this behavior).
// Use the context to cancel the handler completion.
// CloseWithContext does not close the connection manager, just the subscription
// to the connection manager and the consuming goroutines.
func (consumer *Consumer) CloseWithContext(ctx context.Context) {
	consumer.close(ctx, consumer.options.CloseGracefully, nil)
}

// startGoroutines declares the queue if it doesn't exist,
// binds the queue to the routing key(s), and starts the goroutines
// that will consume from the queue
func (consumer *Consumer) startGoroutines(
	handler Handler,
	options ConsumerOptions,
) error {
	consumer.isClosedMu.Lock()
	defer consumer.isClosedMu.Unlock()
	if consumer.isClosed {
		return amqp.ErrClosed
	}
	err := consumer.chanManager.QosSafe(
		options.QOSPrefetch,
		0,
		options.QOSGlobal,
	)
	if err != nil {
		return fmt.Errorf("declare qos failed: %w", err)
	}
	for _, exchangeOption := range options.ExchangeOptions {
		err = declareExchange(consumer.chanManager, exchangeOption)
		if err != nil {
			return fmt.Errorf("declare exchange failed: %w", err)
		}
	}
	err = declareQueue(consumer.chanManager, options.QueueOptions)
	if err != nil {
		return fmt.Errorf("declare queue failed: %w", err)
	}
	err = declareBindings(consumer.chanManager, options)
	if err != nil {
		return fmt.Errorf("declare bindings failed: %w", err)
	}

	msgs, err := consumer.chanManager.ConsumeSafe(
		options.QueueOptions.Name,
		options.RabbitConsumerOptions.Name,
		options.RabbitConsumerOptions.AutoAck,
		options.RabbitConsumerOptions.Exclusive,
		false, // no-local is not supported by RabbitMQ
		options.RabbitConsumerOptions.NoWait,
		tableToAMQPTable(options.RabbitConsumerOptions.Args),
	)
	if err != nil {
		return err
	}

	for range options.Concurrency {
		go handlerGoroutine(consumer, msgs, options, handler)
	}
	consumer.options.Logger.Infof("Processing messages on %v goroutines", options.Concurrency)
	return nil
}

func (consumer *Consumer) getIsClosed() bool {
	consumer.isClosedMu.RLock()
	defer consumer.isClosedMu.RUnlock()
	return consumer.isClosed
}

func handlerGoroutine(consumer *Consumer, msgs <-chan amqp.Delivery, consumeOptions ConsumerOptions, handler Handler) {
	for msg := range msgs {
		if consumer.getIsClosed() {
			break
		}

		if consumeOptions.RabbitConsumerOptions.AutoAck {
			handler(Delivery{msg})
			continue
		}

		switch handler(Delivery{msg}) {
		case Ack:
			err := msg.Ack(false)
			if err != nil {
				consumer.options.Logger.Errorf("can't ack message: %v", err)
			}
		case NackDiscard:
			err := msg.Nack(false, false)
			if err != nil {
				consumer.options.Logger.Errorf("can't nack message: %v", err)
			}
		case NackRequeue:
			err := msg.Nack(false, true)
			if err != nil {
				consumer.options.Logger.Errorf("can't nack message: %v", err)
			}
		}
	}
	consumer.options.Logger.Infof("rabbit consumer goroutine closed")
}

func (consumer *Consumer) waitForHandlerCompletion(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	c := make(chan struct{})
	go func() {
		consumer.handlerMu.Lock()
		defer consumer.handlerMu.Unlock()
		close(c)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c:
		return nil
	}
}
