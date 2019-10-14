package ninep

import (
	"log"
	"os"
)

type Loggable struct {
	ErrorLog, TraceLog Logger
}

func StdLoggable(prefix string) Loggable {
	return Loggable{
		TraceLog: log.New(os.Stdout, prefix, log.LstdFlags),
		ErrorLog: log.New(os.Stderr, prefix, log.LstdFlags),
	}
}

func (l *Loggable) Errorf(format string, values ...interface{}) {
	if l.ErrorLog != nil {
		l.ErrorLog.Printf(format, values...)
	}
}

func (l *Loggable) Tracef(format string, values ...interface{}) {
	if l.TraceLog != nil {
		l.TraceLog.Printf(format, values...)
	}
}
