package rabbitmq

import (
	"fmt"
	"log"

	"github.com/xmapst/go-rabbitmq/internal/logger"
)

// Logger is describes a logging structure. It can be set using
// WithPublisherOptionsLogger() or WithConsumerOptionsLogger().
type Logger logger.Logger

const loggingPrefix = "gorabbit"

type stdDebugLogger struct{}

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

// Tracef -
func (l stdDebugLogger) Tracef(format string, v ...interface{}) {}
