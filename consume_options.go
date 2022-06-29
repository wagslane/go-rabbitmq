package rabbitmq

// getDefaultConsumeOptions describes the options that will be used when a value isn't provided
func getDefaultConsumeOptions(queue string) ConsumeOptions {
	return ConsumeOptions{
		QueueName:         queue,
		Concurrency:       1,
		QOSPrefetch:       0,
		QOSGlobal:         false,
		ConsumerName:      "",
		ConsumerAutoAck:   false,
		ConsumerExclusive: false,
		ConsumerNoWait:    false,
		ConsumerNoLocal:   false,
		ConsumerArgs:      nil,
	}
}

// ConsumeOptions are used to describe how a new consumer will be created.
type ConsumeOptions struct {
	DeclareOptions
	QueueName         string
	Concurrency       int
	QOSPrefetch       int
	QOSGlobal         bool
	ConsumerName      string
	ConsumerAutoAck   bool
	ConsumerExclusive bool
	ConsumerNoWait    bool
	ConsumerNoLocal   bool
	ConsumerArgs      Table
}

// WithConsumeDeclareOptions allows to set declare options that can be used to set up queue, exchange or bindings
// before the consumer process starts.
func WithConsumeDeclareOptions(declareOptionsFuncs ...func(options *DeclareOptions)) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		for _, declareOption := range declareOptionsFuncs {
			// If a queue was set to declare, ensure that the queue name is set.
			if options.DeclareOptions.Queue != nil {
				if options.DeclareOptions.Queue.Name == "" {
					options.DeclareOptions.Queue.Name = options.QueueName
				}
			}

			declareOption(&options.DeclareOptions)
		}

	}
}

// WithConsumeOptionsConcurrency returns a function that sets the concurrency, which means that
// many goroutines will be spawned to run the provided handler on messages
func WithConsumeOptionsConcurrency(concurrency int) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		options.Concurrency = concurrency
	}
}

// WithConsumeOptionsQOSPrefetch returns a function that sets the prefetch count, which means that
// many messages will be fetched from the server in advance to help with throughput.
// This doesn't affect the handler, messages are still processed one at a time.
func WithConsumeOptionsQOSPrefetch(prefetchCount int) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		options.QOSPrefetch = prefetchCount
	}
}

// WithConsumeOptionsQOSGlobal sets the qos on the channel to global, which means
// these QOS settings apply to ALL existing and future
// consumers on all channels on the same connection
func WithConsumeOptionsQOSGlobal(options *ConsumeOptions) {
	options.QOSGlobal = true
}

// WithConsumeOptionsConsumerName returns a function that sets the name on the server of this consumer
// if unset a random name will be given
func WithConsumeOptionsConsumerName(consumerName string) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		options.ConsumerName = consumerName
	}
}

// WithConsumeOptionsConsumerAutoAck returns a function that sets the auto acknowledge property on the server of this consumer
// if unset the default will be used (false)
func WithConsumeOptionsConsumerAutoAck(autoAck bool) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		options.ConsumerAutoAck = autoAck
	}
}

// WithConsumeOptionsConsumerExclusive sets the consumer to exclusive, which means
// the server will ensure that this is the sole consumer
// from this queue. When exclusive is false, the server will fairly distribute
// deliveries across multiple consumers.
func WithConsumeOptionsConsumerExclusive(options *ConsumeOptions) {
	options.ConsumerExclusive = true
}

// WithConsumeOptionsConsumerNoWait sets the consumer to nowait, which means
// it does not wait for the server to confirm the request and
// immediately begin deliveries. If it is not possible to consume, a channel
// exception will be raised and the channel will be closed.
func WithConsumeOptionsConsumerNoWait(options *ConsumeOptions) {
	options.ConsumerNoWait = true
}
