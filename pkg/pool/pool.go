package pool

import (
	"fmt"
	"net"
	"sync"
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

	return poolInst
}
func (p *Pool) dialNew()(*PooledConn, error) {
	netConn,err:= net.Dial("tcp",p.addr)

	if err != nil {
		fmt.Println(" Failed to connect to server", err)
		return nil,err
	}

	return &PooledConn {
		netConn:netConn
	},nil
}
func (p *Pool) Get()(*PooledConn, error) {
select {
case conn := <-p.idle:
	if conn.available{
		conn.available=false
		return conn,nil
	}

default:
	
	rawConn, err := net.Dial("tcp",p.addr)
	if err!= nil {
		fmt.Println(" Failed to connect to server", err)
		return nil,err
	}
return &PooledConn{netConn: rawConn}, nil 
}
}

func (p *Pool) Put()(conn *PooledConn)error {

	conn.available = true	
	select {
		case p.idle <- conn:
			return nil
		default:
			conn.netConn.Close()
			return fmt.Errorf("pool is full")
	}

}

func (p *Pool) Close() error {

}