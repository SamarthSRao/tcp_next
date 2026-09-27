package pool

import (
	"fmt"
	"net"
	"sync"
)

// PooledConn is one backend TCP connection owned by a Pool.
type PooledConn struct {
	NetConn net.Conn
	// available is true while the connection sits in the idle set.
	available bool
	// Startup is opaque session state the caller attaches after this
	// connection has completed a backend handshake. Nil means the TCP
	// connection has not completed startup. The pool never reads it.
	Startup any
}

// Pool is a fixed-size set of pre-dialed TCP connections.
//
// Get blocks until a connection is idle or Close is called. There is no
// checkout timeout. If every dial fails, the idle set is empty and Get
// blocks until Close.
type Pool struct {
	mu      sync.Mutex
	cond    *sync.Cond
	idle    []*PooledConn
	maxSize int
	addr    string
	closed  bool
}

func NewPool(maxSize int, addr string) *Pool {
	poolInst := &Pool{
		idle:    make([]*PooledConn, 0, maxSize),
		maxSize: maxSize,
		addr:    addr,
	}
	poolInst.cond = sync.NewCond(&poolInst.mu)

	// Pre-populate the pool with up to maxSize connections.
	for i := 0; i < maxSize; i++ {
		if conn, err := poolInst.dialNew(); err == nil {
			poolInst.idle = append(poolInst.idle, conn)
		} else {
			fmt.Println("Failed to pre-dial connection:", err)
		}
	}

	return poolInst
}

func (p *Pool) dialNew() (*PooledConn, error) {
	netConn, err := net.Dial("tcp", p.addr)
	if err != nil {
		fmt.Println(" Failed to connect to server", err)
		return nil, err
	}

	return &PooledConn{
		NetConn: netConn,
	}, nil
}

func (p *Pool) Get() (*PooledConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.idle) == 0 && !p.closed {
		p.cond.Wait()
	}
	if len(p.idle) == 0 {
		return nil, fmt.Errorf("pool closed")
	}
	// The connection is now in use.
	conn := p.idle[0]
	p.idle = p.idle[1:]
	conn.available = false
	return conn, nil
}

func (p *Pool) Put(conn *PooledConn) error {
	if conn == nil || conn.NetConn == nil {
		return fmt.Errorf("nil connection")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		conn.NetConn.Close()
		return fmt.Errorf("pool closed")
	}
	// Mark as reusable and return to the idle set.
	conn.available = true
	p.idle = append(p.idle, conn)
	p.cond.Signal()
	return nil
}

// Discard closes a connection that must not be reused and dials a
// replacement so the idle set does not shrink. If the redial fails, the
// pool is left one connection smaller and the error is returned.
func (p *Pool) Discard(conn *PooledConn) error {
	if conn == nil || conn.NetConn == nil {
		return fmt.Errorf("nil connection")
	}
	conn.NetConn.Close()
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return fmt.Errorf("pool closed")
	}
	fresh, err := p.dialNew()
	if err != nil {
		return err
	}
	return p.Put(fresh)
}

func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	for _, conn := range p.idle {
		if conn != nil && conn.NetConn != nil {
			conn.NetConn.Close()
		}
	}
	p.idle = nil
	p.cond.Broadcast()
	return nil
}
