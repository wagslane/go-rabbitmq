package rabbitmq

import (
	"time"
)

// PublishOptions are used to control how data is published
type PublishOptions struct {
	Exchange string
	// Mandatory fails to publish if there are no queues
	// bound to the routing key
	Mandatory bool
	// Immediate fails to publish if there are no consumers
	// that can ack bound to the queue on the routing key
	Immediate bool
	// MIME content type
	ContentType string
	// Transient (0 or 1) or Persistent (2)
	DeliveryMode uint8
	// Expiration time in ms that a message will expire from a queue.
	// See https://www.rabbitmq.com/ttl.html#per-message-ttl-in-publishers
	Expiration string
	// MIME content encoding
	ContentEncoding string
	// 0 to 9
	Priority uint8
	// correlation identifier
	CorrelationID string
	// address to to reply to (ex: RPC)
	ReplyTo string
	// message identifier
	MessageID string
	// message timestamp
	Timestamp time.Time
	// message type name
	Type string
	// creating user id - ex: "guest"
	UserID string
	// creating application id
	AppID string
	// Application or exchange specific fields,
	// the headers exchange will inspect this field.
	Headers Table
}

// WithPublishOptionsExchange returns a function that sets the exchange to publish to
func WithPublishOptionsExchange(exchange string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.Exchange = exchange
	}
}

// WithPublishOptionsMandatory makes the publishing mandatory, which means when a queue is not
// bound to the routing key a message will be sent back on the returns channel for you to handle
func WithPublishOptionsMandatory(options *PublishOptions) {
	options.Mandatory = true
}

// WithPublishOptionsImmediate makes the publishing immediate, which means when a consumer is not available
// to immediately handle the new message, a message will be sent back on the returns channel for you to handle
func WithPublishOptionsImmediate(options *PublishOptions) {
	options.Immediate = true
}

// WithPublishOptionsContentType returns a function that sets the content type, i.e. "application/json"
func WithPublishOptionsContentType(contentType string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.ContentType = contentType
	}
}

// WithPublishOptionsPersistentDelivery sets the message to persist. Transient messages will
// not be restored to durable queues, persistent messages will be restored to
// durable queues and lost on non-durable queues during server restart. By default publishings
// are transient
func WithPublishOptionsPersistentDelivery(options *PublishOptions) {
	options.DeliveryMode = Persistent
}

// WithPublishOptionsExpiration returns a function that sets the expiry/TTL of a message. As per RabbitMq spec, it must be a
// string value in milliseconds.
func WithPublishOptionsExpiration(expiration string) func(options *PublishOptions) {
	return func(options *PublishOptions) {
		options.Expiration = expiration
	}
}

// WithPublishOptionsHeaders returns a function that sets message header values, i.e. "msg-id"
func WithPublishOptionsHeaders(headers Table) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.Headers = headers
	}
}

// WithPublishOptionsContentEncoding returns a function that sets the content encoding, i.e. "utf-8"
func WithPublishOptionsContentEncoding(contentEncoding string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.ContentEncoding = contentEncoding
	}
}

// WithPublishOptionsPriority returns a function that sets the content priority from 0 to 9
func WithPublishOptionsPriority(priority uint8) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.Priority = priority
	}
}

// WithPublishOptionsCorrelationID returns a function that sets the content correlation identifier
func WithPublishOptionsCorrelationID(correlationID string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.CorrelationID = correlationID
	}
}

// WithPublishOptionsReplyTo returns a function that sets the reply to field
func WithPublishOptionsReplyTo(replyTo string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.ReplyTo = replyTo
	}
}

// WithPublishOptionsMessageID returns a function that sets the message identifier
func WithPublishOptionsMessageID(messageID string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.MessageID = messageID
	}
}

// WithPublishOptionsTimestamp returns a function that sets the timestamp for the message
func WithPublishOptionsTimestamp(timestamp time.Time) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.Timestamp = timestamp
	}
}

// WithPublishOptionsType returns a function that sets the message type name
func WithPublishOptionsType(messageType string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.Type = messageType
	}
}

// WithPublishOptionsUserID returns a function that sets the user id i.e. "user"
func WithPublishOptionsUserID(userID string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.UserID = userID
	}
}

// WithPublishOptionsAppID returns a function that sets the application id
func WithPublishOptionsAppID(appID string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.AppID = appID
	}
}
