package proxy

import (
	"context"
	"net"
)

type Proxy struct {
	Addr string
}

func (p *Proxy) Start(ctx context.Context) error {

	Listener, err := net.Listen("tcp", p.Addr)
	if err != nil {
		return err
	}
	defer Listener.Close()

	return nil
}
