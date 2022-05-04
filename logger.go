package rabbitmq

import (
	"fmt"
	"log"
)

// Logger is the interface to send logs to. It can be set using
// WithPublisherOptionsLogger() or WithConsumerOptionsLogger().
type Logger interface {
	Debug(string)
	Info(string)
	Warning(string)
	Error(string)
}

const loggingPrefix = "gorabbit"

// stdLogger logs to stdout using go's default logger.
type stdLogger struct{}

func (l stdLogger) Debug(s string) {
	log.Println(fmt.Sprintf("[Debug] %s: %s", loggingPrefix, s))
}

func (l stdLogger) Info(s string) {
	log.Println(fmt.Sprintf("[Info] %s: %s", loggingPrefix, s))
}

func (l stdLogger) Warning(s string) {
	log.Println(fmt.Sprintf("[Warning] %s: %s", loggingPrefix, s))
}

func (l stdLogger) Error(s string) {
	log.Println(fmt.Sprintf("[Error] %s: %s", loggingPrefix, s))
}
