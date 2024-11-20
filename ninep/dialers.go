package ninep

import (
	"crypto/tls"
	"net"
	"strings"
	"time"
)

func ParseAddr(addr string) (network, address string) {
	network = "tcp"
	address = addr
	unixSocket := strings.TrimPrefix(addr, "unix!")
	if len(unixSocket) != len(addr) {
		network = "unix"
		address = unixSocket
	}
	tcpAddr := strings.TrimPrefix(addr, "tcp!")
	if len(tcpAddr) != len(addr) {
		network = "tcp"
		address = tcpAddr
	}
	if address == "" {
		address = "localhost:564"
	}
	if !strings.Contains(address, ":") {
		address += ":564"
	}
	return
}

type Dialer interface {
	Dial(network, address string) (net.Conn, error)
	Listen(network, address string) (net.Listener, error)
}

type tcpListenerWithKeepAlive struct {
	net.Listener
	KeepAlivePeriod time.Duration
}

func (ln *tcpListenerWithKeepAlive) Accept() (net.Conn, error) {
	tcp, err := ln.Listener.(*net.TCPListener).AcceptTCP()
	if err != nil {
		return nil, err
	}
	if ln.KeepAlivePeriod != 0 {
		if err = tcp.SetKeepAlive(true); err != nil {
			return nil, err
		}
		if err = tcp.SetKeepAlivePeriod(ln.KeepAlivePeriod); err != nil {
			return nil, err
		}
	}
	return tcp, err
}

type TCPDialer struct {
	KeepAlivePeriod time.Duration
}

func (d *TCPDialer) Dial(network, addr string) (net.Conn, error) {
	conn, err := net.Dial(network, addr)
	if err == nil {
		if tcp, ok := conn.(*net.TCPConn); ok && d.KeepAlivePeriod != 0 {
			if err = tcp.SetKeepAlivePeriod(d.KeepAlivePeriod); err != nil {
				return nil, err
			}
			if err = tcp.SetKeepAlive(true); err != nil {
				return nil, err
			}
		}
	}
	return conn, err
}
func (d *TCPDialer) Listen(network, addr string) (net.Listener, error) {
	ln, err := net.Listen(network, addr)
	if err != nil {
		return nil, err
	}
	return &tcpListenerWithKeepAlive{ln, d.KeepAlivePeriod}, err
}

type TLSDialer struct {
	Dialer Dialer
	Config *tls.Config
}

func (d *TLSDialer) Dial(network, addr string) (net.Conn, error) {
	return tls.Dial(network, addr, d.Config)
}
func (d *TLSDialer) Listen(network, addr string) (net.Listener, error) {
	ln, err := d.Dialer.Listen(network, addr)
	if err != nil {
		return nil, err
	}
	return tls.NewListener(ln, d.Config), nil
}
