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

	hs, err := ensureBackendStartup(backendConn, startup)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Backend handshake failed: %v\n", err)
		discardBackend(backendPool, backendConn)
		return
	}

	// Spoof successful auth toward the client (auth bypass for the app).
	// Replay ParameterStatus + BackendKeyData from the real backend.
	if err := pgwire.WriteClientStartupOK(clientConn, hs); err != nil {
		fmt.Fprintf(os.Stderr, "Client handshake write failed: %v\n", err)
		backendPool.Put(backendConn)
		return
	}
	backendPool.Put(backendConn)
	backendConn = nil
	fmt.Printf("Client handshake complete; tunneling queries for %s\n", clientConn.RemoteAddr())

	inTx := false
	for {
		msgType, payload, err := pgwire.ReadPayload(clientConn)
		if err != nil {
			fmt.Printf("Error reading message from client %v", clientConn)
			if backendConn != nil {
				discardBackend(backendPool, backendConn)
			}
			return
		}
		if backendConn == nil {
			conn, borrowErr := backendPool.Get()
			if borrowErr != nil {
				fmt.Printf("Failed to borrow connection: %v\n", borrowErr)
				return
			}
			if _, err := ensureBackendStartup(conn, startup); err != nil {
				fmt.Fprintf(os.Stderr, "Backend handshake failed: %v\n", err)
				discardBackend(backendPool, conn)
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
			msgType, respPayload, err := pgwire.ReadPayload(backendConn.NetConn)
			if err != nil {
				fmt.Printf("Error reading message from backend %v", backendConn)
				discardBackend(backendPool, backendConn)
				return
			}

			clientConn.Write([]byte{msgType})
			lenBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBytes, uint32(len(respPayload)+4))
			clientConn.Write(lenBytes)
			clientConn.Write(respPayload)

			if msgType == 'Z' {
				if len(respPayload) == 0 {
					fmt.Fprintf(os.Stderr, "ReadyForQuery missing status byte\n")
					discardBackend(backendPool, backendConn)
					return
				}
				if respPayload[0] == 'I' {
					inTx = false
				} else {
					inTx = true
				}
				break
			}
		}

		if inTx == false {
			backendPool.Put(backendConn)
			backendConn = nil
		}
	}
}

// ensureBackendStartup completes the backend handshake the first time a
// pooled connection is used, and reuses that handshake afterward.
// A PostgreSQL session accepts StartupMessage only once; sending it again
// on an already-ready connection is a protocol error.
func ensureBackendStartup(conn *pool.PooledConn, startup *pgwire.StartupMessage) (*pgwire.BackendHandshake, error) {
	if conn.Startup != nil {
		hs, ok := conn.Startup.(*pgwire.BackendHandshake)
		if !ok || hs == nil {
			return nil, fmt.Errorf("pooled connection has unexpected startup state")
		}
		return hs, nil
	}
	password := os.Getenv("PGPASSWORD")
	hs, err := pgwire.CompleteBackendStartup(conn.NetConn, startup.Raw, startup.User(), password)
	if err != nil {
		return nil, err
	}
	conn.Startup = hs
	return hs, nil
}

func discardBackend(backendPool *pool.Pool, conn *pool.PooledConn) {
	if err := backendPool.Discard(conn); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to replace backend connection: %v\n", err)
	}
}
