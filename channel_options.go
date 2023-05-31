package rabbitmq

// ChannelOptions are used to describe a channel's configuration.
// Logger is a custom logging interface.
type ChannelOptions struct {
	Logger Logger
}

// getDefaultChannelOptions describes the options that will be used when a value isn't provided
func getDefaultChannelOptions() ChannelOptions {
	return ChannelOptions{
		Logger: stdDebugLogger{},
	}
}

// WithChannelOptionsLogging sets logging to true on the channel options
// and sets the
func WithChannelOptionsLogging(options *ChannelOptions) {
	options.Logger = &stdDebugLogger{}
}

// WithChannelOptionsLogger sets logging to a custom interface.
// Use WithChannelOptionsLogging to just log to stdout.
func WithChannelOptionsLogger(log Logger) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.Logger = log
	}
}
