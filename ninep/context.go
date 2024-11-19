package ninep

type ContextKey string

const (
	SessionKey    ContextKey = "session"
	RawMessageKey ContextKey = "rawMessage"
)
