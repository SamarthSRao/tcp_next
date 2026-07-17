package main

import (
	"fmt"
	"io"
	"net"
	"os"

	"github.com/samarthsrao/tcp-conn-pool/pkg/pgwire"
	"github.com/samarthsrao/tcp-conn-pool/pkg/pool"
)

func main() {
	listener, err := net.Listen("tcp", ":5433")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to listen: %v\n", err)
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Println("Proxy listening on :5433 (TCP-202: startup handshake)...")

	const poolSize = 10
	backendPool := pool.NewPool(poolSize, "localhost:5432")
	defer backendPool.Close()

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to accept: %v\n", err)
			continue
		}
		go handleConnection(clientConn, backendPool)
	}
}

func handleConnection(clientConn net.Conn, backendPool *pool.Pool) {
	defer clientConn.Close()
	type txInfo struct {
		inTx bool
		txBackendConn *pool.PooledConn
	}
	scanner := bufio.NewScanner(clientConn)
	scanner.Split(bufio.ScanBytes)
	for scanner.Scan()
	{
		chunk:= scanner.Bytes()
	}
	
	startup, err := pgwire.ReadStartupPhase(clientConn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Startup phase failed: %v\n", err)
		return
	}
	fmt.Printf("Startup from %s: user=%q database=%q protocol=%d params=%v\n",
		clientConn.RemoteAddr(), startup.User(), startup.Database(),
		startup.ProtocolVersion, startup.Params)

	backendConn, err := backendPool.Get()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to acquire backend: %v\n", err)
		return
	}
	defer backendPool.Put(backendConn)

	password := os.Getenv("PGPASSWORD")
	hs, err := pgwire.CompleteBackendStartup(backendConn.NetConn, startup.Raw, startup.User(), password)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Backend handshake failed: %v\n", err)
		return
	}

	// 3) Spoof successful auth toward the client (auth bypass for the app).
	//    Replay ParameterStatus + BackendKeyData from the real backend.
	if err := pgwire.WriteClientStartupOK(clientConn, hs); err != nil {
		fmt.Fprintf(os.Stderr, "Client handshake write failed: %v\n", err)
		return
	}
	fmt.Printf("Client handshake complete; tunneling queries for %s\n", clientConn.RemoteAddr())

	// 4) Transparent pipe for the rest of the session (queries/results).
	done := make(chan struct{}, 2)

	go func() {
		defer func() { done <- struct{}{} }()
		if _, err := io.Copy(clientConn, backendConn.NetConn); err != nil && err != io.EOF {
			fmt.Fprintf(os.Stderr, "backend→client: %v\n", err)
		}
		_ = clientConn.Close()
	}()

	go func() {
		defer func() { done <- struct{}{} }()
		if _, err := io.Copy(backendConn.NetConn, clientConn); err != nil && err != io.EOF {
			fmt.Fprintf(os.Stderr, "client→backend: %v\n", err)
		}
		_ = backendConn.NetConn.Close()
	}()

	<-done
	<-done
}
