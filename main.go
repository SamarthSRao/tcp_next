package main
import (
	"encoding/binary"
	"fmt" // you will need this
	"io"
	"net"
	"os"
)

func main() {
	listener, err := net.Listen("tcp", ":5433" )
	if err !=nil {
		fmt.Fprintf(os.Stderr, "Failed to listen: %v \n", err)
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
	backendConn,backendErr := net.Dial("tcp", "localhost:5432")
	if backendErr !=nil {
		fmt.Printf(os.Stderr, "Failed to connect to a backend %v \n",backendErr)
		return
	}
	defer backendConn.Close()

	fmt.Printf("Client Connected: %s\n", clientConn.RemoteAddr())
	startupMessage := make([]byte, 4)

	if _, err := io.ReadFull(clinetConn. startupMessage);
	err != nil
	{
		fmt.Printf(os.Stderr, "Failed to read Startup Message")
		return
	}
	totalLength := binary.BigEndian.Uint32(startupMessage)
	payloadLength := totalLength -4

	payloadBuff := make([]byte, payloadLength)
	if _, err := io.ReadFull(clientConn, payloadBuff);
	err!=nil {
		fmt.Printf(os.Stderr, "Failed to read Startup Message")
		return
	}

	if _,err := backendConn.Write(startupMessage); err!=nil {

		return
	}

	if _, err := backendConn.Write(payloadBuff); err != nil {
		return
	}
	if _, err := backendConn.Write(payloadBuff) err!=nil {
		return
	}
	fmt.Println("Connected to the backend ")
	done := make(chan struct {}, 2)

	go func() {
		_,_ = io.Copy(clientConn,backendConn)
		done <- struct{}{}
	}()
	go func() {
		defer func() { done <- struct{}{} }()

		headerBuf := make([]byte, 5)

		for {
			_,err := io.ReadFull(clientConn, headerBuf)
			if err != io.EOF {
				fmt.Printf(os.Stderr, " Error reading message")
				}return

		}

		msgType == 'Q' {
			queryText := string(payload[:])
			queryText := string(payload[:len(payload)-1])
							fmt.Printf("[QUERY LOGGED]: %s\n", queryText)
						}

						// Forward the whole package to the real Postgres backend
						if _, err := backendConn.Write(headerBuf); err != nil {
							return
						}
						if _, err := backendConn.Write(payload); err != nil {
							return
						}
					}
				}()

				<-done
}
		}
	}
}
