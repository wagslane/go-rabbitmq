package logger

// Logger is describes a logging structure. It can be set using
// WithPublisherOptionsLogger() or WithConsumerOptionsLogger().
type Logger interface {
	Fatalf(string, ...interface{})
	Errorf(string, ...interface{})
	Warnf(string, ...interface{})
	Infof(string, ...interface{})
	Debugf(string, ...interface{})
	Tracef(string, ...interface{})
}
