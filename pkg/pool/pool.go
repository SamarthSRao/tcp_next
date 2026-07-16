package pool

import (
 "context"
 "net"
 "fmt"
 "sync"
)

type ConnPool struct {
	netConn net.Conn
	mu       sync.Mutex
	cond     *sync.Cond
	available bool
}

type Pool struct {
	chan *ConnPool
	maxSize int,
	addr string,

}
