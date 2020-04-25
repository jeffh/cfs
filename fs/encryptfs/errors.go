package encryptfs

import "errors"

var ErrInvalidKey = errors.New("Invalid key")
var ErrMissingKey = errors.New("Missing encryption key")
