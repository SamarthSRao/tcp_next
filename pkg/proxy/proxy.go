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

// Start accepts client connections on Addr and copies bytes to BackendAddr
// until ctx is cancelled or Accept fails. It does not speak pgwire; main
// does not use this type.
func (p *Proxy) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", p.Addr)
	if err != nil {
		return err
	}
	defer listener.Close()

	// Close the listener when ctx is cancelled so Accept unblocks.
	// stop lets the goroutine exit when Start returns for another reason.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			listener.Close()
		case <-stop:
		}
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}

		fmt.Printf("Accepted connection from %s \n", conn.RemoteAddr())
		go p.handleConnection(conn)
	}
}

func (p *Proxy) handleConnection(conn net.Conn) {
	defer conn.Close()
	dial, err := net.Dial("tcp", p.BackendAddr)
	if err != nil {
		return
	}
	done := make(chan struct{}, 2)

	defer dial.Close()

	// When either direction finishes, close both sockets. Otherwise a
	// client disconnect leaves the backend copy blocked and this
	// function never returns.
	go func() {
		io.Copy(conn, dial)
		conn.Close()
		dial.Close()
		done <- struct{}{}
	}()

	go func() {
		io.Copy(dial, conn)
		conn.Close()
		dial.Close()
		done <- struct{}{}
	}()
	<-done
	<-done
}
