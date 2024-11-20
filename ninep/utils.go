package ninep

import (
	"errors"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

func CreateLogger(level string, prefix string, L *slog.Logger) *slog.Logger {
	var minLevel slog.Level
	switch level {
	case "debug":
		minLevel = slog.LevelDebug
	case "info":
		minLevel = slog.LevelInfo
	case "warn":
		minLevel = slog.LevelWarn
	case "error", "":
		minLevel = slog.LevelError
	default:
		log.Fatalf("Invalid log level: %q", level)
	}

	if L == nil {
		L = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: minLevel,
		})).WithGroup(prefix)
	} else {
		L = L.WithGroup(prefix)
	}
	return L
}

func Clean(path string) string {
	return "/" + strings.TrimPrefix(filepath.Clean(path), "/")
}

// NextSegment returns the next segment in the path, or "." if it is only the current directory
func NextSegment(path string) string {
	i := strings.Index(path, "/")
	if i == -1 {
		return path
	}
	candidate := path[:i]
	for candidate == "" || candidate == "." {
		path = path[i+1:]
		i = strings.Index(path, "/")
		if i == -1 {
			candidate = path
			break
		}
		candidate = path[:i]
	}
	if candidate == "" {
		return "."
	}
	return candidate
}

// Returns the parent path of the given path, or . if can't go up another directory
func Dirname(path string) string {
	i := strings.LastIndex(path, "/")
	if i == -1 {
		return "."
	}
	return path[:i+1]
}

// Returns the file of the given path
func Basename(path string) string {
	i := strings.LastIndex(path, "/")
	if i == -1 {
		return path
	}
	return path[i+1:]
}

func isClosedSocket(err error) bool {
	return err != nil &&
		(strings.Contains(err.Error(), "use of closed network connection") ||
			errors.Is(err, io.EOF) ||
			errors.Is(err, syscall.EPIPE))
}

func isTimeoutErr(err error) bool {
	if err, ok := err.(net.Error); ok && err.Timeout() {
		return true
	}
	return false
}

func isTemporaryErr(err error) bool {
	type t interface {
		Temporary() bool
	}

	if err, ok := err.(t); ok {
		return err.Temporary()
	} else {
		return false
	}
}

func readUpTo(r io.Reader, p []byte) (int, error) {
	var err error
	n := 0
	for n < len(p) && err == nil {
		m, e := r.Read(p[n:])
		n += m
		if isTimeoutErr(e) {
			return 0, e
		} else if isTemporaryErr(e) {
			continue
		}
		err = e
	}
	return n, err
}

func acceptRversion(L *slog.Logger, rwc net.Conn, txn *cltTransaction, maxMsgSize, minMsgSize uint32) (uint32, error) {
	if L != nil {
		L.Debug("Tversion", slog.Uint64("maxMsgSize", uint64(maxMsgSize)), slog.String("version", VERSION_9P2000))
	}
	txn.req.Tversion(maxMsgSize, VERSION_9P2000)
	if err := txn.req.writeRequest(rwc); err != nil {
		if L != nil {
			L.Error("Tversion.request.failed", slog.Any("error", err))
		}
		return 0, err
	}

	if err := txn.res.readReply(rwc); err != nil {
		if L != nil {
			L.Error("Tversion.response.failed", slog.Any("error", err))
		}
		return 0, err
	}

	request, ok := txn.res.Reply().(Rversion)
	if !ok {
		if L != nil {
			L.Error(
				"Tversion.response.failed.unexpected",
				slog.Uint64("requestType", uint64(txn.req.requestType())),
				slog.String("error", "expected Rversion from server"),
			)
		}
		return 0, ErrBadFormat
	}

	if !strings.HasPrefix(request.Version(), VERSION_9P) {
		if L != nil {
			L.Error("Tversion.response.failed.unsupported", slog.String("version", request.Version()))
		}
		return 0, ErrBadFormat
	}

	size := request.MsgSize()
	if size > maxMsgSize {
		if L != nil {
			L.Error("Tversion.response.failed.msgSizeTooHigh", slog.Uint64("server", uint64(size)), slog.Uint64("client", uint64(maxMsgSize)))
		}
		return 0, ErrBadFormat
	}
	maxMsgSize = request.MsgSize()
	if minMsgSize > maxMsgSize {
		if L != nil {
			L.Error("Tversion.response.failed.msgSizeTooLow", slog.Uint64("server", uint64(size)), slog.Uint64("client", uint64(maxMsgSize)), slog.Uint64("min", uint64(minMsgSize)))
		}
		return 0, ErrBadFormat
	}

	if L != nil {
		L.Debug("Tversion.response.ok", slog.Uint64("msgSize", uint64(maxMsgSize)))
	}

	return maxMsgSize, nil
}

func underlyingError(err error) string {
	for _, e := range mappedErrors {
		if errors.Is(err, e) {
			return e.Error()
		}
	}
	return err.Error()
}
