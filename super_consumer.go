package rabbitmq

import (
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/wagslane/go-rabbitmq/internal/channelmanager"
)

// Consumer with multiple bindings from multiple exchanges
type SuperConsumer struct {
	chanManager                *channelmanager.ChannelManager
	reconnectErrCh             <-chan error
	closeConnectionToManagerCh chan<- struct{}
	options                    SuperConsumerOptions

	isClosedMux *sync.RWMutex
	isClosed    bool
}

// NewConsumer returns a new Consumer connected to the given rabbitmq server
// it also starts consuming on the given connection with automatic reconnection handling
// Do not reuse the returned consumer for anything other than to close it
func NewSuperConsumer(
	conn *Conn,
	handler Handler,
	queue string,
	optionFuncs ...func(*SuperConsumerOptions),
) (*SuperConsumer, error) {
	defaultOptions := getDefaultSuperConsumerOptions(queue)
	options := &defaultOptions
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	if conn.connectionManager == nil {
		return nil, errors.New("connection manager can't be nil")
	}

	chanManager, err := channelmanager.NewChannelManager(conn.connectionManager, options.Logger, conn.connectionManager.ReconnectInterval)
	if err != nil {
		return nil, err
	}
	reconnectErrCh, closeCh := chanManager.NotifyReconnect()

	consumer := &SuperConsumer{
		chanManager:                chanManager,
		reconnectErrCh:             reconnectErrCh,
		closeConnectionToManagerCh: closeCh,
		options:                    *options,
		isClosedMux:                &sync.RWMutex{},
		isClosed:                   false,
	}

	err = consumer.startGoroutines(
		handler,
		*options,
	)
	if err != nil {
		return nil, err
	}

	go func() {
		for err := range consumer.reconnectErrCh {
			consumer.options.Logger.Infof("successful consumer recovery from: %v", err)
			err = consumer.startGoroutines(
				handler,
				*options,
			)
			if err != nil {
				consumer.options.Logger.Fatalf("error restarting consumer goroutines after cancel or close: %v", err)
				consumer.options.Logger.Fatalf("consumer closing, unable to recover")
				return
			}
		}
	}()

	return consumer, nil
}

// Close cleans up resources and closes the consumer.
// It does not close the connection manager, just the subscription
// to the connection manager and the consuming goroutines.
// Only call once.
func (consumer *SuperConsumer) Close() {
	consumer.isClosedMux.Lock()
	defer consumer.isClosedMux.Unlock()
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

// startGoroutines declares the queue if it doesn't exist,
// binds the queue to the routing key(s), and starts the goroutines
// that will consume from the queue
func (consumer *SuperConsumer) startGoroutines(
	handler Handler,
	options SuperConsumerOptions,
) error {
	err := consumer.chanManager.QosSafe(
		options.QOSPrefetch,
		0,
		options.QOSGlobal,
	)
	if err != nil {
		return fmt.Errorf("declare qos failed: %w", err)
	}
	for _, exchangeOption := range options.ExchangeOptionsSlice {
		err = declareSuperExchange(consumer.chanManager, exchangeOption)
	}
	if err != nil {
		return fmt.Errorf("declare exchange failed: %w", err)
	}
	err = declareQueue(consumer.chanManager, options.QueueOptions)
	if err != nil {
		return fmt.Errorf("declare queue failed: %w", err)
	}
	err = declareSuperBindings(consumer.chanManager, options)
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
		go handlerSuperGoroutine(consumer, msgs, options, handler)
	}
	consumer.options.Logger.Infof("Processing messages on %v goroutines", options.Concurrency)
	return nil
}

func (consumer *SuperConsumer) getIsClosed() bool {
	consumer.isClosedMux.RLock()
	defer consumer.isClosedMux.RUnlock()
	return consumer.isClosed
}

func handlerSuperGoroutine(consumer *SuperConsumer, msgs <-chan amqp.Delivery, consumeOptions SuperConsumerOptions, handler Handler) {
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
