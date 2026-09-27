package pool

import (
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestNewPoolDialsMaxSize(t *testing.T) {
	addr, accepted := backendListener(t)
	p := NewPool(3, addr)
	t.Cleanup(func() { p.Close() })

	var conns []net.Conn
	for i := 0; i < 3; i++ {
		conns = append(conns, takeConn(t, accepted))
	}
	select {
	case c := <-accepted:
		t.Fatalf("dialed an extra connection from %v", c.RemoteAddr())
	default:
	}
	for _, c := range conns {
		c.Close()
	}
}

func TestPutGetFIFO(t *testing.T) {
	addr, accepted := backendListener(t)
	p := NewPool(2, addr)
	t.Cleanup(func() { p.Close() })
	takeConn(t, accepted)
	takeConn(t, accepted)

	first, err := p.Get()
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.Get()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Put(first); err != nil {
		t.Fatal(err)
	}
	if err := p.Put(second); err != nil {
		t.Fatal(err)
	}

	got1, err := p.Get()
	if err != nil {
		t.Fatal(err)
	}
	got2, err := p.Get()
	if err != nil {
		t.Fatal(err)
	}
	if got1 != first || got2 != second {
		t.Fatalf("FIFO got %p %p, want %p %p", got1, got2, first, second)
	}
	if got1.available || got2.available {
		t.Fatal("checked-out connections should be marked unavailable")
	}
}

func TestGetBlocksUntilPut(t *testing.T) {
	addr, accepted := backendListener(t)
	p := NewPool(1, addr)
	t.Cleanup(func() { p.Close() })
	takeConn(t, accepted)

	held, err := p.Get()
	if err != nil {
		t.Fatal(err)
	}

	got := make(chan *PooledConn, 1)
	go func() {
		c, err := p.Get()
		if err != nil {
			t.Errorf("Get: %v", err)
			got <- nil
			return
		}
		got <- c
	}()

	select {
	case c := <-got:
		t.Fatalf("Get returned while the only connection was checked out: %v", c)
	case <-time.After(200 * time.Millisecond):
	}

	if err := p.Put(held); err != nil {
		t.Fatal(err)
	}
	select {
	case c := <-got:
		if c != held {
			t.Fatal("expected the connection that was returned")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Get stayed blocked after Put")
	}
}

func TestCloseUnblocksGetAndLeavesCheckedOutConnOpen(t *testing.T) {
	addr, accepted := backendListener(t)
	p := NewPool(1, addr)
	backend := takeConn(t, accepted)

	held, err := p.Get()
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, getErr := p.Get()
		errCh <- getErr
	}()

	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "pool closed") {
			t.Fatalf("got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Get did not return after Close")
	}

	backend.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	buf := make([]byte, 1)
	_, err = backend.Read(buf)
	if err == nil {
		t.Fatal("expected the checked-out connection to stay open")
	}
	if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		t.Fatalf("checked-out connection closed by Pool.Close: %v", err)
	}

	held.NetConn.Close()
	backend.SetReadDeadline(time.Now().Add(time.Second))
	_, err = backend.Read(buf)
	if err != io.EOF {
		t.Fatalf("after explicit close, got %v", err)
	}
}

func TestCloseClosesIdleConnections(t *testing.T) {
	addr, accepted := backendListener(t)
	p := NewPool(1, addr)
	backend := takeConn(t, accepted)

	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	backend.SetDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 1)
	_, err := backend.Read(buf)
	if err != io.EOF {
		t.Fatalf("idle connection still open: %v", err)
	}
}

func TestEmptyPoolGetBlocksUntilClose(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	p := NewPool(2, addr)
	errCh := make(chan error, 1)
	go func() {
		_, getErr := p.Get()
		errCh <- getErr
	}()

	select {
	case err := <-errCh:
		t.Fatalf("Get returned on an empty pool: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "pool closed") {
			t.Fatalf("got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Get did not unblock")
	}
}

func TestDiscardReplacesConnection(t *testing.T) {
	addr, accepted := backendListener(t)
	p := NewPool(1, addr)
	t.Cleanup(func() { p.Close() })
	oldBackend := takeConn(t, accepted)

	held, err := p.Get()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Discard(held); err != nil {
		t.Fatal(err)
	}

	oldBackend.SetDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 1)
	if _, err := oldBackend.Read(buf); err != io.EOF {
		t.Fatalf("discarded connection still open: %v", err)
	}

	replacement := takeConn(t, accepted)
	defer replacement.Close()

	got, err := p.Get()
	if err != nil {
		t.Fatal(err)
	}
	if got == held {
		t.Fatal("Discard returned the closed connection to the pool")
	}
	if _, err := got.NetConn.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
}

func TestDiscardDialFailureShrinksPool(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			accepted <- c
		}
	}()

	p := NewPool(1, addr)
	backend := takeConn(t, accepted)
	defer backend.Close()
	ln.Close()

	held, err := p.Get()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Discard(held); err == nil {
		t.Fatal("expected redial to fail")
	}

	errCh := make(chan error, 1)
	go func() {
		_, getErr := p.Get()
		errCh <- getErr
	}()
	select {
	case err := <-errCh:
		t.Fatalf("Get returned after a failed Discard: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	p.Close()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Get did not unblock after Close")
	}
}

func TestDiscardNil(t *testing.T) {
	p := NewPool(0, "127.0.0.1:1")
	t.Cleanup(func() { p.Close() })
	if err := p.Discard(nil); err == nil {
		t.Fatal("expected error")
	}
}

func TestPutAfterCloseClosesConnection(t *testing.T) {
	addr, accepted := backendListener(t)
	p := NewPool(1, addr)
	backend := takeConn(t, accepted)

	held, err := p.Get()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Put(held); err == nil || !strings.Contains(err.Error(), "pool closed") {
		t.Fatalf("got %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	backend.SetDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 1)
	if _, err := backend.Read(buf); err != io.EOF {
		t.Fatalf("checked-out connection was not closed by Put after Close: %v", err)
	}
}

func backendListener(t *testing.T) (string, <-chan net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	accepted := make(chan net.Conn, 8)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted <- c
		}
	}()
	return ln.Addr().String(), accepted
}

func takeConn(t *testing.T, accepted <-chan net.Conn) net.Conn {
	t.Helper()
	select {
	case c := <-accepted:
		t.Cleanup(func() { c.Close() })
		return c
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for backend connection")
		return nil
	}
}
