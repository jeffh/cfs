package ninep

import (
	"fmt"
	"math/rand"
	"net"
	"time"
)

type timeoutErr struct {
	Duration time.Duration
}

func (e *timeoutErr) Timeout() bool { return true }
func (e *timeoutErr) Error() string { return fmt.Sprintf("Flaky timeout: %s", e.Duration) }

type FlakyConn struct {
	net.Conn
	readDeadline  *time.Time
	writeDeadline *time.Time

	DropConnectionChance int // 0-100
	MinDelay             time.Duration
	MaxDelay             time.Duration
	ReadOnTimeout        bool
	WriteOnTimeout       bool
}

func (c *FlakyConn) delay() time.Duration {
	return time.Duration(rand.Int63n(int64(c.MaxDelay))) + c.MinDelay
}

func (c *FlakyConn) Read(b []byte) (n int, err error) {
	d := c.delay()
	if c.readDeadline != nil {
		now := time.Now()
		expectedDeadline := now.Add(d)
		if expectedDeadline.Before(*c.readDeadline) {
			time.Sleep(c.readDeadline.Sub(now))
			if c.ReadOnTimeout {
				n, err = c.Conn.Read(b)
				if err == nil {
					err = &timeoutErr{d}
				}
				return n, err
			} else {
				return 0, &timeoutErr{d}
			}
		} else {
			// sleep
		}
	} else {
		// sleep
	}
	time.Sleep(d)
	return c.Conn.Read(b)
}

// Write writes data to the connection.
// Write can be made to time out and return an Error with Timeout() == true
// after a fixed time limit; see SetDeadline and SetWriteDeadline.
func (c *FlakyConn) Write(b []byte) (n int, err error) {
	d := c.delay()
	now := time.Now()
	expectedDeadline := now.Add(d)
	if expectedDeadline.Before(*c.readDeadline) {
		if c.writeDeadline != nil {
			time.Sleep(c.writeDeadline.Sub(now))
			if c.WriteOnTimeout {
				n, err = c.Conn.Write(b)
				if err == nil {
					err = &timeoutErr{d}
				}
				return
			} else {
				return 0, &timeoutErr{d}
			}
		} else {
			// sleep
		}
	} else {
		// sleep
	}
	time.Sleep(d)
	return c.Conn.Write(b)
}

// func (c *FlakyConn) Close() error { return c.Client.Close() }

func (c *FlakyConn) SetDeadline(t time.Time) error {
	err := c.SetDeadline(t)
	if err == nil {
		c.readDeadline = &t
		c.writeDeadline = &t
	}
	return err
}

func (c *FlakyConn) SetReadDeadline(t time.Time) error {
	err := c.SetReadDeadline(t)
	if err == nil {
		c.readDeadline = &t
	}
	return err
}
func (c *FlakyConn) SetWriteDeadline(t time.Time) error {
	err := c.SetWriteDeadline(t)
	if err == nil {
		c.writeDeadline = &t
	}
	return err
}
