package proxy

import (
	"context"
	"fmt"
	"io"
	"net"
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
	defer Listener.Close()

	for {
		conn, err := Listener.Accept()
		if err != nil {
			return err
		}

		fmt.Printf("Accepted connectoin from %s \n", conn.RemoteAddr())
		go p.handleConnection(conn)
	}

	return nil
}

func (p *Proxy) handleConnection(conn net.Conn) {
	defer conn.Close()
	dial, err := net.Dial("tcp", p.BackendAddr)
	if err != nil {
		return
	}
	done := make(chan struct{}, 2)

	defer dial.Close()

	go func() {
		io.Copy(conn, dial)
		conn.Close()
		done <- struct{}{}
	}()

	go func() {
		io.Copy(dial, conn)
		done <- struct{}{}
	}()
	<-done
	<-done

}
