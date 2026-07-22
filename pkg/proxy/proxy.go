package proxy

import (
	"context"
	"net"
	"fmt"
)

type Proxy struct {
	Addr        string
	BackendAddr string
}

func (p *Proxy) Start(ctx context.Context) error {

	Listener, err := net.Listen("tcp", p.Addr)
	if err != nil {
		return err
	}
	for {
		conn, err := Listener.Accept()
		if err != nil {
			return err
		}
		defer Listener.Close()
		fmt.Printf(client net.Conn)
		go p.handleConnection(client net.Conn)
	}
	

	return nil
}
