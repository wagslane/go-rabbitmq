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

// stdLogger logs to stdout using go's default logger.
type stdLogger struct{}

func (l stdLogger) Printf(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s: %s", loggingPrefix, format), v...)
}
