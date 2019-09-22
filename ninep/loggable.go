package ninep

type Loggable struct {
	ErrorLog, TraceLog Logger
}

func (l *Loggable) errorf(format string, values ...interface{}) {
	if l.ErrorLog != nil {
		l.ErrorLog.Printf(format, values...)
	}
}

func (l *Loggable) tracef(format string, values ...interface{}) {
	if l.TraceLog != nil {
		l.TraceLog.Printf(format, values...)
	}
}
