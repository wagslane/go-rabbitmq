package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/makometr/go-rabbitmq/internal/channelmanager"
	"github.com/makometr/go-rabbitmq/internal/connectionmanager"
	amqp "github.com/rabbitmq/amqp091-go"
)

// DeliveryMode. Transient means higher throughput but messages will not be
// restored on broker restart. The delivery mode of publishings is unrelated
// to the durability of the queues they reside on. Transient messages will
// not be restored to durable queues, persistent messages will be restored to
// durable queues and lost on non-durable queues during server restart.
//
// This remains typed as uint8 to match Publishing.DeliveryMode. Other
// delivery modes specific to custom queue implementations are not enumerated
// here.
const (
	Transient  uint8 = amqp.Transient
	Persistent uint8 = amqp.Persistent
)

// Return captures a flattened struct of fields returned by the server when a
// Publishing is unable to be delivered either due to the `mandatory` flag set
// and no route found, or `immediate` flag set and no free consumer.
type Return struct {
	amqp.Return
}

// Confirmation notifies the acknowledgment or negative acknowledgement of a publishing identified by its delivery tag.
// Use NotifyPublish to consume these events. ReconnectionCount is useful in that each time it increments, the DeliveryTag
// is reset to 0, meaning you can use ReconnectionCount+DeliveryTag to ensure uniqueness
type Confirmation struct {
	amqp.Confirmation
	ReconnectionCount int
}

// Publisher allows you to publish messages safely across an open connection
type Publisher struct {
	chanManager                *channelmanager.ChannelManager
	connManager                *connectionmanager.ConnectionManager
	reconnectErrCh             <-chan error
	closeConnectionToManagerCh chan<- struct{}

	disablePublishDueToFlow   bool
	disablePublishDueToFlowMu *sync.RWMutex

	disablePublishDueToBlocked   bool
	disablePublishDueToBlockedMu *sync.RWMutex

	handlerMu            *sync.Mutex
	notifyReturnHandler  func(r Return)
	notifyPublishHandler func(p Confirmation)

	options PublisherOptions
}

type PublisherConfirmation []*amqp.DeferredConfirmation

// NewPublisher returns a new publisher with an open channel to the cluster.
// If you plan to enforce mandatory or immediate publishing, those failures will be reported
// on the channel of Returns that you should setup a listener on.
// Flow controls are automatically handled as they are sent from the server, and publishing
// will fail with an error when the server is requesting a slowdown
func NewPublisher(conn *Conn, optionFuncs ...func(*PublisherOptions)) (*Publisher, error) {
	defaultOptions := getDefaultPublisherOptions()
	options := &defaultOptions
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	if conn.connectionManager == nil {
		return nil, errors.New("connection manager can't be nil")
	}

	chanManager, err := channelmanager.NewChannelManager(conn.connectionManager, options.ConfirmMode, options.Logger, conn.connectionManager.ReconnectInterval)
	if err != nil {
		return nil, err
	}

	reconnectErrCh, closeCh := chanManager.NotifyReconnect()
	publisher := &Publisher{
		chanManager:                  chanManager,
		connManager:                  conn.connectionManager,
		reconnectErrCh:               reconnectErrCh,
		closeConnectionToManagerCh:   closeCh,
		disablePublishDueToFlow:      false,
		disablePublishDueToFlowMu:    &sync.RWMutex{},
		disablePublishDueToBlocked:   false,
		disablePublishDueToBlockedMu: &sync.RWMutex{},
		handlerMu:                    &sync.Mutex{},
		notifyReturnHandler:          nil,
		notifyPublishHandler:         nil,
		options:                      *options,
	}

	err = publisher.startup()
	if err != nil {
		return nil, err
	}

	if options.ConfirmMode {
		publisher.NotifyPublish(func(_ Confirmation) {
			// set a blank handler to set the channel in confirm mode
		})
	}

	go func() {
		for err := range publisher.reconnectErrCh {
			publisher.options.Logger.Infof("successful publisher recovery from: %v", err)
			err := publisher.startup()
			if err != nil {
				publisher.options.Logger.Fatalf("error on startup for publisher after cancel or close: %v", err)
				publisher.options.Logger.Fatalf("publisher closing, unable to recover")
				return
			}
			publisher.startReturnHandler()
			publisher.startPublishHandler()
		}
	}()

	return publisher, nil
}

func (publisher *Publisher) startup() error {
	err := declareExchange(publisher.chanManager, publisher.options.ExchangeOptions)
	if err != nil {
		return fmt.Errorf("declare exchange failed: %w", err)
	}
	go publisher.startNotifyFlowHandler()
	go publisher.startNotifyBlockedHandler()
	return nil
}

/*
Publish publishes the provided data to the given routing keys over the connection.
*/
func (publisher *Publisher) Publish(
	data []byte,
	routingKeys []string,
	optionFuncs ...func(*PublishOptions),
) error {
	return publisher.PublishWithContext(context.Background(), data, routingKeys, optionFuncs...)
}

// PublishWithContext publishes the provided data to the given routing keys over the connection.
func (publisher *Publisher) PublishWithContext(
	ctx context.Context,
	data []byte,
	routingKeys []string,
	optionFuncs ...func(*PublishOptions),
) error {
	publisher.disablePublishDueToFlowMu.RLock()
	defer publisher.disablePublishDueToFlowMu.RUnlock()
	if publisher.disablePublishDueToFlow {
		return fmt.Errorf("publishing blocked due to high flow on the server")
	}

	publisher.disablePublishDueToBlockedMu.RLock()
	defer publisher.disablePublishDueToBlockedMu.RUnlock()
	if publisher.disablePublishDueToBlocked {
		return fmt.Errorf("publishing blocked due to TCP block on the server")
	}

	options := &PublishOptions{}
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}
	if options.DeliveryMode == 0 {
		options.DeliveryMode = Transient
	}

	for _, routingKey := range routingKeys {
		message := amqp.Publishing{}
		message.ContentType = options.ContentType
		message.DeliveryMode = options.DeliveryMode
		message.Body = data
		message.Headers = tableToAMQPTable(options.Headers)
		message.Expiration = options.Expiration
		message.ContentEncoding = options.ContentEncoding
		message.Priority = options.Priority
		message.CorrelationId = options.CorrelationID
		message.ReplyTo = options.ReplyTo
		message.MessageId = options.MessageID
		message.Timestamp = options.Timestamp
		message.Type = options.Type
		message.UserId = options.UserID
		message.AppId = options.AppID

		// Actual publish.
		err := publisher.chanManager.PublishWithContextSafe(
			ctx,
			options.Exchange,
			routingKey,
			options.Mandatory,
			options.Immediate,
			message,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// PublishWithContext publishes the provided data to the given routing keys over the connection.
// if the publisher is in confirm mode (which can be either done by calling `NotifyPublish` with a custom handler
// or by using `WithPublisherOptionsConfirm`) a publisher confirmation is returned.
// This confirmation can be used to check if the message was actually published or wait for this to happen.
// If the publisher is not in confirm mode, the returned confirmation will always be nil.
func (publisher *Publisher) PublishWithDeferredConfirmWithContext(
	ctx context.Context,
	data []byte,
	routingKeys []string,
	optionFuncs ...func(*PublishOptions),
) (PublisherConfirmation, error) {
	publisher.disablePublishDueToFlowMu.RLock()
	defer publisher.disablePublishDueToFlowMu.RUnlock()
	if publisher.disablePublishDueToFlow {
		return nil, fmt.Errorf("publishing blocked due to high flow on the server")
	}

	publisher.disablePublishDueToBlockedMu.RLock()
	defer publisher.disablePublishDueToBlockedMu.RUnlock()
	if publisher.disablePublishDueToBlocked {
		return nil, fmt.Errorf("publishing blocked due to TCP block on the server")
	}

	options := &PublishOptions{}
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}
	if options.DeliveryMode == 0 {
		options.DeliveryMode = Transient
	}

	var deferredConfirmations []*amqp.DeferredConfirmation

	for _, routingKey := range routingKeys {
		message := amqp.Publishing{}
		message.ContentType = options.ContentType
		message.DeliveryMode = options.DeliveryMode
		message.Body = data
		message.Headers = tableToAMQPTable(options.Headers)
		message.Expiration = options.Expiration
		message.ContentEncoding = options.ContentEncoding
		message.Priority = options.Priority
		message.CorrelationId = options.CorrelationID
		message.ReplyTo = options.ReplyTo
		message.MessageId = options.MessageID
		message.Timestamp = options.Timestamp
		message.Type = options.Type
		message.UserId = options.UserID
		message.AppId = options.AppID

		// Actual publish.
		conf, err := publisher.chanManager.PublishWithDeferredConfirmWithContextSafe(
			ctx,
			options.Exchange,
			routingKey,
			options.Mandatory,
			options.Immediate,
			message,
		)
		if err != nil {
			return nil, err
		}
		deferredConfirmations = append(deferredConfirmations, conf)
	}

	return deferredConfirmations, nil
}

// Close closes the publisher and releases resources
// The publisher should be discarded as it's not safe for re-use
// Only call Close() once
func (publisher *Publisher) Close() {
	// close the channel so that rabbitmq server knows that the
	// publisher has been stopped.
	err := publisher.chanManager.Close()
	if err != nil {
		publisher.options.Logger.Warnf("error while closing the channel: %v", err)
	}
	publisher.options.Logger.Infof("closing publisher...")
	go func() {
		publisher.closeConnectionToManagerCh <- struct{}{}
	}()
}

// NotifyReturn registers a listener for basic.return methods.
// These can be sent from the server when a publish is undeliverable either from the mandatory or immediate flags.
// These notifications are shared across an entire connection, so if you're creating multiple
// publishers on the same connection keep that in mind
func (publisher *Publisher) NotifyReturn(handler func(r Return)) {
	publisher.handlerMu.Lock()
	start := publisher.notifyReturnHandler == nil
	publisher.notifyReturnHandler = handler
	publisher.handlerMu.Unlock()

	if start {
		publisher.startReturnHandler()
	}
}

// NotifyPublish registers a listener for publish confirmations, must set ConfirmPublishings option
// These notifications are shared across an entire connection, so if you're creating multiple
// publishers on the same connection keep that in mind
func (publisher *Publisher) NotifyPublish(handler func(p Confirmation)) {
	publisher.handlerMu.Lock()
	shouldStart := publisher.notifyPublishHandler == nil
	publisher.notifyPublishHandler = handler
	publisher.handlerMu.Unlock()

	if shouldStart {
		publisher.startPublishHandler()
	}
}

func (publisher *Publisher) startReturnHandler() {
	publisher.handlerMu.Lock()
	if publisher.notifyReturnHandler == nil {
		publisher.handlerMu.Unlock()
		return
	}
	publisher.handlerMu.Unlock()

	go func() {
		returns := publisher.chanManager.NotifyReturnSafe(make(chan amqp.Return, 1))
		for ret := range returns {
			go publisher.notifyReturnHandler(Return{ret})
		}
	}()
}

func (publisher *Publisher) startPublishHandler() {
	publisher.handlerMu.Lock()
	if publisher.notifyPublishHandler == nil {
		publisher.handlerMu.Unlock()
		return
	}
	publisher.handlerMu.Unlock()
	publisher.chanManager.ConfirmSafe(false)

	go func() {
		confirmationCh := publisher.chanManager.NotifyPublishSafe(make(chan amqp.Confirmation, 1))
		for conf := range confirmationCh {
			go publisher.notifyPublishHandler(Confirmation{
				Confirmation:      conf,
				ReconnectionCount: int(publisher.chanManager.GetReconnectionCount()),
			})
		}
	}()
}
