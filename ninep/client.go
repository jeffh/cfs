package ninep

import (
	"crypto/tls"
	"net"
	"time"
)

type Client struct {
	conn net.Conn
	txns chan *cltTransaction

	User string

	Timeout                 time.Duration
	MaxMsgSize              uint32
	MaxSimultaneousRequests uint
}

func (c *Client) ConnectTLS(addr string, tlsCfg *tls.Config) error {
	var err error
	c.conn, err = tls.Dial("tcp", addr, tlsCfg)
	if err != nil {
		return err
	}
	if err = c.acceptVersion(); err != nil {
		return err
	}
	return nil
}

func (c *Client) Connect(addr string) error {
	var err error
	c.conn, err = net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	if err = c.acceptVersion(); err != nil {
		return err
	}
	return nil
}

func (c *Client) acceptVersion() error {
	{ // set default values
		if c.MaxMsgSize < MIN_MESSAGE_SIZE {
			c.MaxMsgSize = DEFAULT_MAX_MESSAGE_SIZE
		}

		if c.MaxSimultaneousRequests == 0 {
			c.MaxSimultaneousRequests = 1
		}
	}

	{ // initialization
		c.txns = make(chan *cltTransaction, c.MaxSimultaneousRequests)
		go func() {
			for i := uint(0); i < c.MaxSimultaneousRequests; i++ {
				t := createClientTransaction(Tag(i), c.MaxMsgSize)
				c.txns <- &t
			}
		}()
	}

	// handshake
	// version := VERSION_9P2000

	return nil
}

// import (
// 	"fmt"
// 	"net"
// )

// func clientConnect(conn net.Conn) (msize uint32, e error) {
// 	size := DEFAULT_MAX_MESSAGE_SIZE
// 	version := VERSION_9P2000

// 	_, err := WriteMessage(conn, MakeReqVersion(size, version))
// 	if err != nil {
// 		return 0, err
// 	}

// 	m, err := ReadMessage(conn)
// 	if err != nil {
// 		return 0, err
// 	}

// 	reply, ok := m.(RepVersion)
// 	if !ok {
// 		return 0, ErrBadFormat
// 	}

// 	if reply.MsgSize() > size {
// 		return 0, fmt.Errorf("Server returned larger max message size than supported (got: %d, wanted: %d)", reply.MsgSize(), size)
// 	}

// 	if reply.MsgSize() < MIN_MESSAGE_SIZE {
// 		return 0, fmt.Errorf("Server returned below minimum  message size than supported (got: %d, min: %d)", reply.MsgSize(), MIN_MESSAGE_SIZE)
// 	}

// 	if reply.Version() != version {
// 		return 0, fmt.Errorf("Server returned unsupported 9P version (got: %#v, wanted: %#v)", reply.Version(), version)
// 	}

// 	// we're good
// 	return reply.MsgSize(), nil
// }
