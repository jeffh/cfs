package encryptfs

import "errors"

var ErrInvalidKey = errors.New("invalid key")
var ErrMissingKey = errors.New("missing encryption key")
