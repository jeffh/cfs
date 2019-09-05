package ninep

import (
	"fmt"
	"net"
)

type BackingSystem interface{}

type Server struct{}

type serverSession struct {
	c          net.Conn
	maxMsgSize uint32
}

func (s *Server) receive(conn net.Conn) (*serverSession, error) {
	preferredSize := DEFAULT_MAX_MESSAGE_SIZE
	version := VERSION_9P2000

	m, err := ReadMessage(conn)
	if err != nil {
		return 0, err
	}

	request, ok := m.(ReqVersion)
	if !ok {
		conn.Close()
		return nil, ErrBadFormat
	}

	var size uint32
	if request.MsgSize() > preferredSize {
		size = preferredSize
	} else {
		size = request.MsgSize()
	}

	if reply.Tag() != NO_TAG {
		conn.Close()
		return nil, fmt.Errorf("Client sent bad tag (got: %d, wanted: NO_TAG/%d)", reply.Tag(), NO_TAG)
	}

	if reply.MsgSize() < MIN_MESSAGE_SIZE {
		conn.Close()
		return nil, fmt.Errorf("Client returned below minimum message size than supported (got: %d, min: %d)", reply.MsgSize(), MIN_MESSAGE_SIZE)
	}

	if request.Version() != version {
		conn.Close()
		return nil, fmt.Errorf("Client requested unsupported 9P version (got: %#v, wanted: %#v)", request.Version(), version)
	}

	_, err := WriteMessage(w.r, MakeRepVersion(size, version))
	if err != nil {
		conn.Close()
		return nil, err
	}

	// we're good
	return &serverSession{conn, size}, nil
}

func (s *serverSession) messageLoop() {
	for {
		msg, err := ReadMessage(s.c)
		if err != nil {
			s.c.Close()
			return
		}
		err = s.handle(msg)
		if err != nil {
			s.c.Close()
			return
		}
	}
}

func (s *serverSession) writeError(msg string) error {
	_, err := WriteMessage(MakeRepError(tag, msg))
	if err != nil {
		s.c.Close()
	}
	return err
}

func (s *serverSession) handle(m Message) error {
	switch m.Type() {
	case Tauth:
	case Tflush:
	case Twalk:
	case Topen:
	case Tcreate:
	case Tread:
	case Twrite:
	case Tclunk:
	case Tremove:
	case Tstat:
	case Twstat:
	default:
		s.writeError("Unsupported message")
		return ErrBadFormat
	}
	return nil
}
