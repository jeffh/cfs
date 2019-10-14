package ninep

import (
	"crypto/tls"
	"net"
)

type Dialer interface {
	Dial(network, address string) (net.Conn, error)
	Listen(network, address string) (net.Listener, error)
}

type TCPDialer struct{}

func (d *TCPDialer) Dial(network, addr string) (net.Conn, error) { return net.Dial(network, addr) }
func (d *TCPDialer) Listen(network, addr string) (net.Listener, error) {
	return net.Listen(network, addr)
}

type TLSDialer struct {
	Dialer Dialer
	Config tls.Config
}

func (d *TLSDialer) Dial(network, addr string) (net.Conn, error) {
	return tls.Dial(network, addr, &d.Config)
}
func (d *TLSDialer) Listen(network, addr string) (net.Listener, error) {
	ln, err := d.Dialer.Listen(network, addr)
	if err != nil {
		return nil, err
	}
	return tls.NewListener(ln, &d.Config), nil
}

type AANDialer struct{}

func (d *AANDialer) Dial(network, addr string) (net.Conn, error) { return DialAan(network, addr) }
func (d *AANDialer) Listen(network, addr string) (net.Listener, error) {
	return ListenAan(network, addr)
}
