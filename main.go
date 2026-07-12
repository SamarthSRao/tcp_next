package main

import (
	"fmt"
	"io"
	"net"
	"os"

	"github.com/samarthsrao/tcp-conn-pool/pkg/pgwire"
)

func main() {
	listener, err := net.Listen("tcp", ":5433")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to listen: %v\n", err)
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Println("Proxy listening on :5433 (TCP-202: startup handshake)...")

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to accept: %v\n", err)
			continue
		}
		go handleConnection(clientConn)
	}
}

func handleConnection(clientConn net.Conn) {
	defer clientConn.Close()

	// 1) Client startup phase (optional SSLRequest → 'N', then StartupMessage).
	startup, err := pgwire.ReadStartupPhase(clientConn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Startup phase failed: %v\n", err)
		return
	}
	fmt.Printf("Startup from %s: user=%q database=%q protocol=%d params=%v\n",
		clientConn.RemoteAddr(), startup.User(), startup.Database(),
		startup.ProtocolVersion, startup.Params)

	// 2) Dial real Postgres and complete its handshake (proxy owns backend auth).
	backendConn, err := net.Dial("tcp", "localhost:5432")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to dial backend: %v\n", err)
		return
	}
	defer backendConn.Close()

	password := os.Getenv("PGPASSWORD")
	hs, err := pgwire.CompleteBackendStartup(backendConn, startup.Raw, startup.User(), password)
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
		if _, err := io.Copy(clientConn, backendConn); err != nil && err != io.EOF {
			fmt.Fprintf(os.Stderr, "backend→client: %v\n", err)
		}
		_ = clientConn.Close()
	}()

	go func() {
		defer func() { done <- struct{}{} }()
		if _, err := io.Copy(backendConn, clientConn); err != nil && err != io.EOF {
			fmt.Fprintf(os.Stderr, "client→backend: %v\n", err)
		}
		_ = backendConn.Close()
	}()

	<-done
	<-done
}
