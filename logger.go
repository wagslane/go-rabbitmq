package rabbitmq

import (
	"fmt"
	"log"
)

// Logger is the interface to send logs to. It can be set using
// WithPublisherOptionsLogger() or WithConsumerOptionsLogger().
type Logger interface {
	Printf(string, ...interface{})
}

const loggingPrefix = "gorabbit"

// stdlog logs to stdout using go's default logger.
type stdlog struct{}

func (l stdlog) Printf(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s: %s", loggingPrefix, format), v...)
}

// nolog does not log at all, this is the default.
type nolog struct{}

func (l nolog) Printf(format string, v ...interface{}) {}
