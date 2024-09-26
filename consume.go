package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/makometr/go-rabbitmq/internal/channelmanager"
	amqp "github.com/rabbitmq/amqp091-go"
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

	chanManager, err := channelmanager.NewChannelManager(conn.connectionManager, false, options.Logger, conn.connectionManager.ReconnectInterval)
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
	}

	return consumer, nil
}

// Run starts consuming with automatic reconnection handling. Do not reuse the
// consumer for anything other than to close it.
func (consumer *Consumer) Run(handler Handler) error {
	handlerWrapper := func(d Delivery) (action Action) {
		if !consumer.handlerMu.TryRLock() {
			return NackRequeue
		}
		defer consumer.handlerMu.RUnlock()
		return handler(d)
	}

	err := consumer.startGoroutines(
		handlerWrapper,
		consumer.options,
	)
	if err != nil {
		return err
	}

	for err := range consumer.reconnectErrCh {
		consumer.options.Logger.Infof("successful consumer recovery from: %v", err)
		err = consumer.startGoroutines(
			handlerWrapper,
			consumer.options,
		)
		if err != nil {
			return fmt.Errorf("error restarting consumer goroutines after cancel or close: %w", err)
		}
	}

	return nil
}

// Close cleans up resources and closes the consumer.
// It waits for handler to finish before returning by default
// (use WithConsumerOptionsForceShutdown option to disable this behavior).
// Use CloseWithContext to specify a context to cancel the handler completion.
// It does not close the connection manager, just the subscription
// to the connection manager and the consuming goroutines.
// Only call once.
func (consumer *Consumer) Close() {
	if consumer.options.CloseGracefully {
		consumer.options.Logger.Infof("waiting for handler to finish...")
		err := consumer.waitForHandlerCompletion(context.Background())
		if err != nil {
			consumer.options.Logger.Warnf("error while waiting for handler to finish: %v", err)
		}
	}

	consumer.cleanupResources()

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
// Only call once.
func (consumer *Consumer) CloseWithContext(ctx context.Context) {
	if consumer.options.CloseGracefully {
		err := consumer.waitForHandlerCompletion(ctx)
		if err != nil {
			consumer.options.Logger.Warnf("error while waiting for handler to finish: %v", err)
		}
	}

	consumer.cleanupResources()
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

	for i := 0; i < options.Concurrency; i++ {
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
