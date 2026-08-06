package main

import (
	"encoding/binary"
	"fmt"
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
		inTx          bool
		txBackendConn *pool.PooledConn
	}

	startup, err := pgwire.ReadStartupPhase(clientConn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Startup phase failed: %v\n", err)
		return
	}
	fmt.Printf("Startup from %s: user=%q database=%q protocol=%d params=%v\n",
		clientConn.RemoteAddr(), startup.User(), startup.Database(),
		startup.ProtocolVersion, startup.Params)
	//this is where I need to change the session

	backendConn, err := backendPool.Get()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to acquire backend: %v\n", err)
		return
	}

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
	backendPool.Put(backendConn)
	backendConn = nil
	fmt.Printf("Client handshake complete; tunneling queries for %s\n", clientConn.RemoteAddr())

	// 4) Transparent pipe for the rest of the session (queries/results).

	for {

		msgType, payload, err := pgwire.ReadMessage(clientConn)
		if err != nil {
			fmt.Printf("Error reading message from client %v", clientConn)
			return
		}
		if backendConn == nil {

			conn, borrowErr := backendPool.Get()
			if borrowErr != nil {
				fmt.Printf("Failed to borrow connection: %v\n", borrowErr)
				return
			}
			backendConn = conn
		}
		backendConn.NetConn.Write([]byte{msgType})
		lenBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBytes, uint32(len(payload)+4))

		backendConn.NetConn.Write(lenBytes)
		backendConn.NetConn.Write(payload)

		for {
			msgType, respPayload, err := pgwire.ReadMessage(backendConn.NetConn)
			if err != nil {
				fmt.Printf("Error reading message from backend %v", backendConn)
				return
			}

			clientConn.Write([]byte{msgType})
			lenBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBytes, uint32(len(respPayload)+4))
			clientConn.Write(lenBytes)
			clientConn.Write(respPayload)

			if msgType == 'Z' {

				break

			}
		}

	}

}
