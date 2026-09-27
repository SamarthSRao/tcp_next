package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestHandleConnectionRelaysBytes(t *testing.T) {
	backendLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backendLn.Close()
	go func() {
		c, err := backendLn.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		io.Copy(c, c)
	}()

	clientLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer clientLn.Close()

	done := make(chan struct{})
	go func() {
		c, err := clientLn.Accept()
		if err != nil {
			return
		}
		(&Proxy{BackendAddr: backendLn.Addr().String()}).handleConnection(c)
		close(done)
	}()

	client, err := net.Dial("tcp", clientLn.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.SetDeadline(time.Now().Add(2 * time.Second))

	if _, err := client.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(client, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "ping" {
		t.Fatalf("got %q", buf)
	}
	client.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConnection did not return")
	}
}

func TestHandleConnectionBackendDownClosesClient(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	clientLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer clientLn.Close()

	done := make(chan struct{})
	go func() {
		c, err := clientLn.Accept()
		if err != nil {
			return
		}
		(&Proxy{BackendAddr: addr}).handleConnection(c)
		close(done)
	}()

	client, err := net.Dial("tcp", clientLn.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.SetDeadline(time.Now().Add(2 * time.Second))

	buf := make([]byte, 1)
	_, err = client.Read(buf)
	if err == nil {
		t.Fatal("expected client to be closed")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConnection did not return")
	}
}

func TestStartRelaysBytesAndStopsOnCancel(t *testing.T) {
	backendLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backendLn.Close()
	go func() {
		c, err := backendLn.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		io.Copy(c, c)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	p := &Proxy{Addr: freeAddr(t), BackendAddr: backendLn.Addr().String()}
	errCh := make(chan error, 1)
	go func() {
		errCh <- p.Start(ctx)
	}()

	var client net.Conn
	deadline := time.Now().Add(2 * time.Second)
	for {
		c, err := net.Dial("tcp", p.Addr)
		if err == nil {
			client = c
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("proxy did not listen: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	defer client.Close()
	client.SetDeadline(time.Now().Add(2 * time.Second))

	if _, err := client.Write([]byte("xyz")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 3)
	if _, err := io.ReadFull(client, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "xyz" {
		t.Fatalf("got %q", buf)
	}

	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Start returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after cancel")
	}
}

func TestStartAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p := &Proxy{Addr: freeAddr(t), BackendAddr: "127.0.0.1:1"}
	err := p.Start(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestStartBadAddr(t *testing.T) {
	p := &Proxy{Addr: "127.0.0.1:not-a-port", BackendAddr: "127.0.0.1:1"}
	if err := p.Start(context.Background()); err == nil {
		t.Fatal("expected listen error")
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}
