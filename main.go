package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	listener, err := net.Listen("tcp", ":5433")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to listen: %v\n", err)
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Println("Proxy listening to :5433 - waiting for connections...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Proxy failed to accept: %v\n", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(clientConn net.Conn) {
	defer clientConn.Close()
	fmt.Printf("Client connected: %s\n", clientConn.RemoteAddr())

	backendConn, err := net.Dial("tcp", "localhost:5432")
	if err != nil {
		fmt.Printf("failed to connect to backend: %v\n", err)
		return
	}
	defer backendConn.Close()

	fmt.Printf("Connected to backend for client: %s\n", clientConn.RemoteAddr())

	// Channel to signal when copying is done
	done := make(chan struct{}, 2)

	// Pipe data from client to backend
	go func() {
		io.Copy(backendConn, clientConn)
		done <- struct{}{}
	}()

	// Pipe data from backend to client
	go func() {
		io.Copy(clientConn, backendConn)
		done <- struct{}{}
	}()

	// Wait for one side to close the connection
	<-done
	fmt.Printf("Connection closed: %s\n", clientConn.RemoteAddr())
}
