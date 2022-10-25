package rabbitmq

import (
	"fmt"
	"sync"
	"time"

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

// Confirmation notifies the acknowledgment or negative acknowledgment of a publishing identified by its delivery tag.
// Use NotifyPublish to consume these events. ReconnectionCount is useful in that each time it increments, the DeliveryTag
// is reset to 0, meaning you can use ReconnectionCount+DeliveryTag to ensure uniqueness
type Confirmation struct {
	amqp.Confirmation
	ReconnectionCount int
}

// Publisher allows you to publish messages safely across an open connection
type Publisher struct {
	chManager *channelManager

	notifyReturnChan  chan Return
	notifyPublishChan chan Confirmation

	disablePublishDueToFlow    bool
	disablePublishDueToFlowMux *sync.RWMutex

	disablePublishDueToBlocked    bool
	disablePublishDueToBlockedMux *sync.RWMutex

	options PublisherOptions
}

// PublisherOptions are used to describe a publisher's configuration.
// Logger is a custom logging interface.
type PublisherOptions struct {
	Logger            Logger
	ReconnectInterval time.Duration
	ExchangeOptions   *ExchangeOptions
}

// WithPublisherOptionsExchangeName returns a function that sets the exchange to publish to
func WithPublisherOptionsExchangeName(name string) func(*PublisherOptions) {
	return func(options *PublisherOptions) {
		getPublisherExchangeOptionsOrSetDefault(options).Name = name
	}
}

// WithPublisherOptionsExchangeKind returns a function that sets the binding exchange kind/type
func WithPublisherOptionsExchangeKind(kind string) func(*PublisherOptions) {
	return func(options *PublisherOptions) {
		getPublisherExchangeOptionsOrSetDefault(options).Kind = kind
	}
}

// WithPublisherOptionsExchangeDurable returns a function that sets the binding exchange durable flag
func WithPublisherOptionsExchangeDurable(options *PublisherOptions) {
	getPublisherExchangeOptionsOrSetDefault(options).Durable = true
}

// WithPublisherOptionsExchangeAutoDelete returns a function that sets the binding exchange autoDelete flag
func WithPublisherOptionsExchangeAutoDelete(options *PublisherOptions) {
	getPublisherExchangeOptionsOrSetDefault(options).AutoDelete = true
}

// WithPublisherOptionsExchangeInternal returns a function that sets the binding exchange internal flag
func WithPublisherOptionsExchangeInternal(options *PublisherOptions) {
	getPublisherExchangeOptionsOrSetDefault(options).Internal = true
}

// WithPublisherOptionsExchangeNoWait returns a function that sets the binding exchange noWait flag
func WithPublisherOptionsExchangeNoWait(options *PublisherOptions) {
	getPublisherExchangeOptionsOrSetDefault(options).NoWait = true
}

// WithPublisherOptionsExchangeArgs returns a function that sets the binding exchange arguments that are specific to the server's implementation of the exchange
func WithPublisherOptionsExchangeArgs(args Table) func(*PublisherOptions) {
	return func(options *PublisherOptions) {
		getPublisherExchangeOptionsOrSetDefault(options).ExchangeArgs = args
	}
}

// WithPublisherOptionsExchangeDeclare returns a function that declares the binding exchange.
// Use this setting if you want the publisher to create the exchange on start.
func WithPublisherOptionsExchangeDeclare(options *PublisherOptions) {
	getPublisherExchangeOptionsOrSetDefault(options).Declare = true
}

// WithPublisherOptionsReconnectInterval sets the interval at which the publisher will
// attempt to reconnect to the rabbit server
func WithPublisherOptionsReconnectInterval(reconnectInterval time.Duration) func(options *PublisherOptions) {
	return func(options *PublisherOptions) {
		options.ReconnectInterval = reconnectInterval
	}
}

// WithPublisherOptionsLogging sets logging to true on the publisher options
// and sets the
func WithPublisherOptionsLogging(options *PublisherOptions) {
	options.Logger = &stdDebugLogger{}
}

// WithPublisherOptionsLogger sets logging to a custom interface.
// Use WithPublisherOptionsLogging to just log to stdout.
func WithPublisherOptionsLogger(log Logger) func(options *PublisherOptions) {
	return func(options *PublisherOptions) {
		options.Logger = log
	}
}

// NewPublisher returns a new publisher with an open channel to the cluster.
// If you plan to enforce mandatory or immediate publishing, those failures will be reported
// on the channel of Returns that you should setup a listener on.
// Flow controls are automatically handled as they are sent from the server, and publishing
// will fail with an error when the server is requesting a slowdown
func NewPublisher(url string, config Config, optionFuncs ...func(*PublisherOptions)) (*Publisher, error) {
	options := &PublisherOptions{
		Logger:            &stdDebugLogger{},
		ReconnectInterval: time.Second * 5,
		ExchangeOptions:   getDefaultExchangeOptions(),
	}
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	chManager, err := newChannelManager(url, config, options.Logger, options.ReconnectInterval)
	if err != nil {
		return nil, err
	}

	publisher := &Publisher{
		chManager:                     chManager,
		disablePublishDueToFlow:       false,
		disablePublishDueToFlowMux:    &sync.RWMutex{},
		disablePublishDueToBlocked:    false,
		disablePublishDueToBlockedMux: &sync.RWMutex{},
		options:                       *options,
		notifyReturnChan:              nil,
		notifyPublishChan:             nil,
	}

	if err = declareOrVerifyExchange(publisher.options.ExchangeOptions, chManager.channel); err != nil {
		return nil, err
	}

	go publisher.startNotifyFlowHandler()
	go publisher.startNotifyBlockedHandler()

	go publisher.handleRestarts()

	return publisher, nil
}

// NotifyReturn registers a listener for basic.return methods.
// These can be sent from the server when a publish is undeliverable either from the mandatory or immediate flags.
func (publisher *Publisher) NotifyReturn() <-chan Return {
	publisher.notifyReturnChan = make(chan Return)
	go publisher.startNotifyReturnHandler()
	return publisher.notifyReturnChan
}

// NotifyPublish registers a listener for publish confirmations, must set ConfirmPublishings option
func (publisher *Publisher) NotifyPublish() <-chan Confirmation {
	publisher.notifyPublishChan = make(chan Confirmation)
	publisher.startNotifyPublishHandler()
	return publisher.notifyPublishChan
}

// Publish publishes the provided data to the given routing keys over the connection
func (publisher *Publisher) Publish(
	data []byte,
	routingKeys []string,
	optionFuncs ...func(*PublishOptions),
) error {
	publisher.disablePublishDueToFlowMux.RLock()
	defer publisher.disablePublishDueToFlowMux.RUnlock()
	if publisher.disablePublishDueToFlow {
		return fmt.Errorf("publishing blocked due to high flow on the server")
	}

	publisher.disablePublishDueToBlockedMux.RLock()
	defer publisher.disablePublishDueToBlockedMux.RUnlock()
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
		var message = amqp.Publishing{}
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
		err := publisher.chManager.channel.Publish(
			publisher.options.ExchangeOptions.Name,
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

// Close closes the publisher and releases resources
// The publisher should be discarded as it's not safe for re-use
func (publisher Publisher) Close() error {
	publisher.chManager.logger.Infof("closing publisher...")
	return publisher.chManager.close()
}

func (publisher *Publisher) handleRestarts() {
	for err := range publisher.chManager.notifyCancelOrClose {
		publisher.options.Logger.Infof("successful publisher recovery from: %v", err)
		go publisher.startNotifyFlowHandler()
		go publisher.startNotifyBlockedHandler()
		if publisher.notifyReturnChan != nil {
			go publisher.startNotifyReturnHandler()
		}
		if publisher.notifyPublishChan != nil {
			publisher.startNotifyPublishHandler()
		}
	}
}

func (publisher *Publisher) startNotifyReturnHandler() {
	returnAMQPCh := publisher.chManager.channel.NotifyReturn(make(chan amqp.Return, 1))
	for ret := range returnAMQPCh {
		publisher.notifyReturnChan <- Return{ret}
	}
}

func (publisher *Publisher) startNotifyPublishHandler() {
	publisher.chManager.channel.Confirm(false)
	go func() {
		publishAMQPCh := publisher.chManager.channel.NotifyPublish(make(chan amqp.Confirmation, 1))
		for conf := range publishAMQPCh {
			publisher.notifyPublishChan <- Confirmation{
				Confirmation:      conf,
				ReconnectionCount: int(publisher.chManager.reconnectionCount),
			}
		}
	}()
}
