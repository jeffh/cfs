package ninep

func IsTemporaryErr(err error) bool {
	type t interface {
		Temporary() bool
	}

	if err, ok := err.(t); ok {
		return err.Temporary()
	} else {
		return false
	}
}
