package rabbitmq

import (
	"fmt"
	"log"
)

// Logger is the interface to send logs to. It can be set using
// WithPublisherOptionsLogger() or WithConsumerOptionsLogger().
type Logger interface {
	FatalF(string, ...interface{})
	ErrorF(string, ...interface{})
	WarnF(string, ...interface{})
	InfoF(string, ...interface{})
	DebugF(string, ...interface{})
	TraceF(string, ...interface{})
}

const loggingPrefix = "gorabbit"

// stdDebugLogger logs to stdout up to the `DebugF` level
type stdDebugLogger struct{}

func (l stdDebugLogger) FatalF(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s FATAL: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) ErrorF(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s ERROR: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) WarnF(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s WARN: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) InfoF(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s INFO: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) DebugF(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s DEBUG: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) TraceF(format string, v ...interface{}) {}
