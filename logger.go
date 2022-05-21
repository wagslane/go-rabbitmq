package rabbitmq

import (
	"fmt"
	"log"
)

// Logger is the interface to send logs to. It can be set using
// WithPublisherOptionsLogger() or WithConsumerOptionsLogger().
type Logger interface {
	Fatalf(string, ...interface{})
	Errorf(string, ...interface{})
	Warnf(string, ...interface{})
	Infof(string, ...interface{})
	Debugf(string, ...interface{})
	Tracef(string, ...interface{})
}

const loggingPrefix = "gorabbit"

// stdDebugLogger logs to stdout up to the `DebugF` level
type stdDebugLogger struct{}

func (l stdDebugLogger) Fatalf(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s FATAL: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) Errorf(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s ERROR: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) Warnf(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s WARN: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) Infof(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s INFO: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) Debugf(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s DEBUG: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) Tracef(format string, v ...interface{}) {}
