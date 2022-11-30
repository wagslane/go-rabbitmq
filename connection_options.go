package rabbitmq

import "time"

// ConnectionOptions are used to describe how a new consumer will be created.
type ConnectionOptions struct {
	QOSPrefetch       int
	QOSGlobal         bool
	ReconnectInterval time.Duration
	Logger            Logger
	Config            Config
}

// getDefaultConnectionOptions describes the options that will be used when a value isn't provided
func getDefaultConnectionOptions() ConnectionOptions {
	return ConnectionOptions{
		QOSPrefetch:       0,
		QOSGlobal:         false,
		ReconnectInterval: time.Second * 5,
		Logger:            stdDebugLogger{},
		Config:            Config{},
	}
}

// WithConnectionOptionsQOSPrefetch returns a function that sets the prefetch count, which means that
// many messages will be fetched from the server in advance to help with throughput.
// This doesn't affect the handler, messages are still processed one at a time.
func WithConnectionOptionsQOSPrefetch(prefetchCount int) func(*ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.QOSPrefetch = prefetchCount
	}
}

// WithConnectionOptionsQOSGlobal sets the qos on the channel to global, which means
// these QOS settings apply to ALL existing and future
// consumers on all channels on the same connection
func WithConnectionOptionsQOSGlobal(options *ConnectionOptions) {
	options.QOSGlobal = true
}

// WithConnectionOptionsReconnectInterval sets the reconnection interval
func WithConnectionOptionsReconnectInterval(interval time.Duration) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.ReconnectInterval = interval
	}
}

// WithConnectionOptionsLogging sets logging to true on the consumer options
// and sets the
func WithConnectionOptionsLogging(options *ConnectionOptions) {
	options.Logger = stdDebugLogger{}
}

// WithConnectionOptionsLogger sets logging to true on the consumer options
// and sets the
func WithConnectionOptionsLogger(log Logger) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.Logger = log
	}
}

// WithConnectionOptionsConfig sets the Config used in the connection
func WithConnectionOptionsConfig(cfg Config) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.Config = cfg
	}
}
