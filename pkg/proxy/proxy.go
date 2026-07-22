package proxy

import (
	"context"
	"net"
)

type Proxy struct {
	Addr string
}

func (p *Proxy) Start(ctx context.Context) error {

	Listener, err := net.Dial("tcp", p.add)
}
