package pool

import (
	"fmt"
	"net"
)

type PooledConn struct {
	netConn   net.Conn
	available bool
}

type Pool struct {
	idle    chan *PooledConn
	maxSize int

	addr    string
}

func NewPool(maxSize int, addr string) *Pool {

	poolInst := &Pool{
		idle:    make(chan *PooledConn, maxSize),
		maxSize: maxSize,
		addr:    addr,
	}

	// Pre‑populate the pool with maxSize connections.
	for i := 0; i < maxSize; i++ {
		if conn, err := poolInst.dialNew(); err == nil {
			poolInst.idle <- conn
		} else {
			fmt.Println("Failed to pre‑dial connection:", err)
		}
	}

	return poolInst
}
func (p *Pool) dialNew()(*PooledConn, error) {
	netConn,err:= net.Dial("tcp",p.addr)

	if err != nil {
		fmt.Println(" Failed to connect to server", err)
		return nil,err
	}

	return &PooledConn{
		netConn: netConn,
	}, nil
}
func (p *Pool) Get()(*PooledConn, error) {
    // Block until a connection is available in the idle channel.
    conn := <-p.idle
    if conn == nil {
        return nil, fmt.Errorf("pool closed")
    }
    // The connection is now in‑use.
    conn.available = false
    return conn, nil
}

func (p *Pool) Put(conn *PooledConn) error {
    // Mark as reusable and return to the idle channel.
    conn.available = true
    p.idle <- conn
    return nil
}

func (p *Pool) Close() error {
	close(p.idle)
	for conn := range p.idle {
		conn.netConn.Close()
	}
	return nil
}