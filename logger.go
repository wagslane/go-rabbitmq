package rabbitmq

import (
	"fmt"
	"log"
	"os"

	"github.com/makometr/go-rabbitmq/internal/logger"
)

// Logger is describes a logging structure. It can be set using
// WithPublisherOptionsLogger() or WithConsumerOptionsLogger().
type Logger logger.Logger

const loggingPrefix = "gorabbit"

type stdDebugLogger struct{}

// Fatalf -
func (l stdDebugLogger) Fatalf(format string, v ...interface{}) {
	log.Fatalf(fmt.Sprintf("%s FATAL: %s", loggingPrefix, format), v...)
}

// Errorf -
func (l stdDebugLogger) Errorf(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s ERROR: %s", loggingPrefix, format), v...)
}

// Warnf -
func (l stdDebugLogger) Warnf(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s WARN: %s", loggingPrefix, format), v...)
}

// Infof -
func (l stdDebugLogger) Infof(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s INFO: %s", loggingPrefix, format), v...)
}

// Debugf -
func (l stdDebugLogger) Debugf(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s DEBUG: %s", loggingPrefix, format), v...)
}

// simpleLogF is used to support logging in the test functions.
// This could be exposed publicly for integration in more simple logging interfaces.
type simpleLogF func(string, ...interface{})

func (l simpleLogF) Fatalf(format string, v ...interface{}) {
	l(fmt.Sprintf("%s FATAL: %s", loggingPrefix, format), v...)
	os.Exit(1)
}

func (l simpleLogF) Errorf(format string, v ...interface{}) {
	l(fmt.Sprintf("%s ERROR: %s", loggingPrefix, format), v...)
}

func (l simpleLogF) Warnf(format string, v ...interface{}) {
	l(fmt.Sprintf("%s WARN: %s", loggingPrefix, format), v...)
}

func (l simpleLogF) Infof(format string, v ...interface{}) {
	l(fmt.Sprintf("%s INFO: %s", loggingPrefix, format), v...)
}

func (l simpleLogF) Debugf(format string, v ...interface{}) {
	l(fmt.Sprintf("%s DEBUG: %s", loggingPrefix, format), v...)
}
