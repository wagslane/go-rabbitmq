package rabbitmq

import (
	"fmt"
	"log"
)

type logger struct {
	logging bool
}

const loggingPrefix = "gorabbit"

func (l logger) Printf(format string, v ...interface{}) {
	if l.logging {
		log.Printf(fmt.Sprintf("%s: %s", loggingPrefix, format), v...)
	}
}

func (l logger) Println(v ...interface{}) {
	if l.logging {
		log.Println(loggingPrefix, fmt.Sprintf("%v", v...))
	}
}
