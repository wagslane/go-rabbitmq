package rabbitmq

import "time"

// ConnectionOptions are used to describe how a new consumer will be created.
type ConnectionOptions struct {
	BaseReconnectInterval time.Duration
	Logger                Logger
	Config                Config
}

// getDefaultConnectionOptions describes the options that will be used when a value isn't provided
func getDefaultConnectionOptions() ConnectionOptions {
	return ConnectionOptions{
		BaseReconnectInterval: time.Second * 5,
		Logger:                stdDebugLogger{},
		Config:                Config{},
	}
}

// WithConnectionOptionsBaseReconnectInterval sets the base reconnection interval.
// Consecutive failed attempts back off exponentially from this value, capped
// at 16x, with up to 25% jitter.
func WithConnectionOptionsBaseReconnectInterval(interval time.Duration) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.BaseReconnectInterval = interval
	}
}

// WithConnectionOptionsLogging uses a default logger that writes to std out
func WithConnectionOptionsLogging(options *ConnectionOptions) {
	options.Logger = stdDebugLogger{}
}

// WithConnectionOptionsLogger sets logging to a custom interface.
// Use WithConnectionOptionsLogging to just log to stdout.
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
