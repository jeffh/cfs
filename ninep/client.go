package ninep

import (
	"fmt"
	"net"
)

func clientConnect(conn net.Conn) (msize uint32, e error) {
	size := DEFAULT_MAX_MESSAGE_SIZE
	version := VERSION_9P2000

	_, err := WriteMessage(conn, MakeReqVersion(size, version))
	if err != nil {
		return 0, err
	}

	m, err := ReadMessage(conn)
	if err != nil {
		return 0, err
	}

	reply, ok := m.(RepVersion)
	if !ok {
		return 0, ErrBadFormat
	}

	if reply.MsgSize() > size {
		return 0, fmt.Errorf("Server returned larger max message size than supported (got: %d, wanted: %d)", reply.MsgSize(), size)
	}

	if reply.MsgSize() < MIN_MESSAGE_SIZE {
		return 0, fmt.Errorf("Server returned below minimum  message size than supported (got: %d, min: %d)", reply.MsgSize(), MIN_MESSAGE_SIZE)
	}

	if reply.Version() != version {
		return 0, fmt.Errorf("Server returned unsupported 9P version (got: %#v, wanted: %#v)", reply.Version(), version)
	}

	// we're good
	return reply.MsgSize(), nil
}
