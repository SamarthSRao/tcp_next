package main

import (
	"encoding/binary"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samarthsrao/tcp-conn-pool/pkg/pgwire"
	"github.com/samarthsrao/tcp-conn-pool/pkg/pool"
)

func TestSecondClientReusesStartedBackend(t *testing.T) {
	addr, accepted := listenBackend(t)
	p := pool.NewPool(1, addr)
	t.Cleanup(func() { p.Close() })
	backend := takeBackend(t, accepted)
	proxyAddr := serveProxy(t, p)

	var startups atomic.Int32
	go func() {
		started := false
		for {
			kind, _, err := readPacket(backend)
			if err != nil {
				return
			}
			if kind == "startup" {
				if started {
					startups.Add(1)
					return
				}
				started = true
				startups.Add(1)
				if err := writeAuthOK(backend); err != nil {
					return
				}
				continue
			}
			if kind == "Q" {
				if err := writeResult(backend, 'I'); err != nil {
					return
				}
			}
		}
	}()

	alice := dialProxy(t, proxyAddr)
	writeStartup(t, alice, "alice", "db")
	readHandshake(t, alice)

	bob := dialProxy(t, proxyAddr)
	writeStartup(t, bob, "bob", "other")
	readHandshake(t, bob)

	if got := startups.Load(); got != 1 {
		t.Fatalf("backend startups = %d, want 1", got)
	}

	writeQuery(t, alice, "SELECT 1")
	expectReady(t, alice, 'I')
	writeQuery(t, bob, "SELECT 2")
	expectReady(t, bob, 'I')
}

func TestQueryOnVirginConnectionStartsItFirst(t *testing.T) {
	addr, accepted := listenBackend(t)
	p := pool.NewPool(2, addr)
	t.Cleanup(func() { p.Close() })
	c1 := takeBackend(t, accepted)
	c2 := takeBackend(t, accepted)
	proxyAddr := serveProxy(t, p)

	events := make(chan string, 8)
	go pumpBackend(c1, "c1", events)
	go pumpBackend(c2, "c2", events)

	client := dialProxy(t, proxyAddr)
	writeStartup(t, client, "alice", "db")
	readHandshake(t, client)

	started := map[string]bool{}
	ev := recvEvent(t, events)
	id, kind, _ := splitEvent(ev)
	if kind != "startup" {
		t.Fatalf("first event %q", ev)
	}
	started[id] = true

	writeQuery(t, client, "SELECT 1")

	deadline := time.After(3 * time.Second)
	gotQuery := false
	for !gotQuery {
		select {
		case ev := <-events:
			id, kind, payload := splitEvent(ev)
			switch kind {
			case "startup":
				if started[id] {
					t.Fatalf("second startup on %s", id)
				}
				started[id] = true
			case "Q":
				if !started[id] {
					t.Fatalf("query before startup on %s (%q)", id, payload)
				}
				gotQuery = true
			default:
				t.Fatalf("unexpected event %q", ev)
			}
		case <-deadline:
			t.Fatal("timed out waiting for query")
		}
	}
	if len(started) != 2 {
		t.Fatalf("started connections = %d, want 2", len(started))
	}
	expectReady(t, client, 'I')
}

func TestTransactionHoldsBackendUntilIdle(t *testing.T) {
	addr, accepted := listenBackend(t)
	p := pool.NewPool(1, addr)
	t.Cleanup(func() { p.Close() })
	backend := takeBackend(t, accepted)
	proxyAddr := serveProxy(t, p)

	alice := dialProxy(t, proxyAddr)
	writeStartup(t, alice, "alice", "db")
	kind, _, err := readPacket(backend)
	if err != nil {
		t.Fatal(err)
	}
	if kind != "startup" {
		t.Fatalf("got %s", kind)
	}
	if err := writeAuthOK(backend); err != nil {
		t.Fatal(err)
	}
	readHandshake(t, alice)

	bob := dialProxy(t, proxyAddr)
	writeStartup(t, bob, "bob", "db")
	readHandshake(t, bob)

	writeQuery(t, alice, "HOLD")
	kind, payload, err := readPacket(backend)
	if err != nil {
		t.Fatal(err)
	}
	if kind != "Q" || !strings.Contains(payload, "HOLD") {
		t.Fatalf("got %s %q", kind, payload)
	}
	if err := writeResult(backend, 'T'); err != nil {
		t.Fatal(err)
	}
	expectReady(t, alice, 'T')

	bobDone := make(chan struct{})
	go func() {
		defer close(bobDone)
		writeQuery(t, bob, "FROM_B")
		expectReady(t, bob, 'I')
	}()

	// Give the proxy time to read Bob's query and block in Pool.Get.
	time.Sleep(50 * time.Millisecond)
	backend.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, _, err = readPacket(backend)
	if err == nil {
		t.Fatal("backend got a message while the connection was held in a transaction")
	}
	if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		t.Fatalf("expected timeout while connection was held, got %v", err)
	}
	backend.SetDeadline(time.Now().Add(5 * time.Second))

	writeQuery(t, alice, "RELEASE")
	kind, payload, err = readPacket(backend)
	if err != nil {
		t.Fatal(err)
	}
	if kind != "Q" || !strings.Contains(payload, "RELEASE") {
		t.Fatalf("while held, backend got %s %q, want RELEASE", kind, payload)
	}
	if err := writeResult(backend, 'I'); err != nil {
		t.Fatal(err)
	}
	expectReady(t, alice, 'I')

	kind, payload, err = readPacket(backend)
	if err != nil {
		t.Fatal(err)
	}
	if kind != "Q" || !strings.Contains(payload, "FROM_B") {
		t.Fatalf("after release, backend got %s %q", kind, payload)
	}
	if err := writeResult(backend, 'I'); err != nil {
		t.Fatal(err)
	}

	select {
	case <-bobDone:
	case <-time.After(2 * time.Second):
		t.Fatal("bob's query did not finish after the backend was released")
	}
}

func TestFailedTransactionKeepsBackend(t *testing.T) {
	addr, accepted := listenBackend(t)
	p := pool.NewPool(1, addr)
	t.Cleanup(func() { p.Close() })
	backend := takeBackend(t, accepted)
	proxyAddr := serveProxy(t, p)

	client := dialProxy(t, proxyAddr)
	writeStartup(t, client, "alice", "db")
	if kind, _, err := readPacket(backend); err != nil || kind != "startup" {
		t.Fatalf("startup: kind=%s err=%v", kind, err)
	}
	if err := writeAuthOK(backend); err != nil {
		t.Fatal(err)
	}
	readHandshake(t, client)

	writeQuery(t, client, "BROKEN")
	if _, _, err := readPacket(backend); err != nil {
		t.Fatal(err)
	}
	if err := writeResult(backend, 'E'); err != nil {
		t.Fatal(err)
	}
	expectReady(t, client, 'E')

	// The only backend is checked out, so a second checkout blocks
	// until a later ReadyForQuery reports idle.
	got := make(chan *pool.PooledConn, 1)
	go func() {
		c, err := p.Get()
		if err != nil {
			t.Errorf("Get: %v", err)
			return
		}
		got <- c
	}()
	select {
	case c := <-got:
		t.Fatalf("pool had an idle connection during a failed transaction: %v", c)
	case <-time.After(150 * time.Millisecond):
	}

	writeQuery(t, client, "DONE")
	if kind, payload, err := readPacket(backend); err != nil || kind != "Q" || !strings.Contains(payload, "DONE") {
		t.Fatalf("backend got kind=%s payload=%q err=%v", kind, payload, err)
	}
	if err := writeResult(backend, 'I'); err != nil {
		t.Fatal(err)
	}
	expectReady(t, client, 'I')

	select {
	case c := <-got:
		if err := p.Put(c); err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("connection was not released after ReadyForQuery idle")
	}
}

func TestConcurrentIdleQueriesShareOneBackend(t *testing.T) {
	addr, accepted := listenBackend(t)
	p := pool.NewPool(1, addr)
	t.Cleanup(func() { p.Close() })
	backend := takeBackend(t, accepted)
	proxyAddr := serveProxy(t, p)

	var startups atomic.Int32
	var queries atomic.Int32
	go func() {
		started := false
		for {
			kind, _, err := readPacket(backend)
			if err != nil {
				return
			}
			if kind == "startup" {
				if started {
					startups.Add(100)
					return
				}
				started = true
				startups.Add(1)
				if err := writeAuthOK(backend); err != nil {
					return
				}
				continue
			}
			if kind == "Q" {
				queries.Add(1)
				if err := writeResult(backend, 'I'); err != nil {
					return
				}
			}
		}
	}()

	alice := dialProxy(t, proxyAddr)
	bob := dialProxy(t, proxyAddr)
	writeStartup(t, alice, "alice", "db")
	readHandshake(t, alice)
	writeStartup(t, bob, "bob", "db")
	readHandshake(t, bob)

	var wg sync.WaitGroup
	for _, c := range []net.Conn{alice, bob} {
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				writeQuery(t, c, "SELECT 1")
				expectReady(t, c, 'I')
			}
		}(c)
	}
	wg.Wait()

	if got := startups.Load(); got != 1 {
		t.Fatalf("startups = %d, want 1", got)
	}
	if got := queries.Load(); got != 10 {
		t.Fatalf("queries = %d, want 10", got)
	}
}

func TestHandshakeErrorReplacesBackend(t *testing.T) {
	addr, accepted := listenBackend(t)
	p := pool.NewPool(1, addr)
	t.Cleanup(func() { p.Close() })
	backend := takeBackend(t, accepted)
	proxyAddr := serveProxy(t, p)

	client := dialProxy(t, proxyAddr)
	writeStartup(t, client, "alice", "db")
	if kind, _, err := readPacket(backend); err != nil || kind != "startup" {
		t.Fatalf("startup: kind=%s err=%v", kind, err)
	}
	var payload []byte
	payload = append(payload, 'M')
	payload = append(payload, []byte("nope")...)
	payload = append(payload, 0, 0)
	if err := pgwire.WriteMessage(backend, 'E', payload); err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 8)
	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := client.Read(buf); err == nil {
		t.Fatal("client received bytes after a failed backend handshake")
	}

	replacement := takeBackend(t, accepted)
	defer replacement.Close()

	got := make(chan *pool.PooledConn, 1)
	go func() {
		c, err := p.Get()
		if err != nil {
			t.Errorf("Get: %v", err)
			return
		}
		got <- c
	}()
	select {
	case c := <-got:
		if c == nil {
			t.Fatal("nil replacement")
		}
		p.Put(c)
	case <-time.After(2 * time.Second):
		t.Fatal("pool did not recover a connection after the failed handshake")
	}
}

func TestEmptyReadyForQueryDoesNotPanic(t *testing.T) {
	addr, accepted := listenBackend(t)
	p := pool.NewPool(1, addr)
	t.Cleanup(func() { p.Close() })
	backend := takeBackend(t, accepted)
	proxyAddr := serveProxy(t, p)

	client := dialProxy(t, proxyAddr)
	writeStartup(t, client, "alice", "db")
	if kind, _, err := readPacket(backend); err != nil || kind != "startup" {
		t.Fatalf("startup: kind=%s err=%v", kind, err)
	}
	if err := writeAuthOK(backend); err != nil {
		t.Fatal(err)
	}
	readHandshake(t, client)

	writeQuery(t, client, "SELECT 1")
	if kind, _, err := readPacket(backend); err != nil || kind != "Q" {
		t.Fatalf("query: kind=%s err=%v", kind, err)
	}
	// ReadyForQuery with length 4 and no status byte.
	var hdr [5]byte
	hdr[0] = 'Z'
	binary.BigEndian.PutUint32(hdr[1:], 4)
	if _, err := backend.Write(hdr[:]); err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 8)
	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _ = client.Read(buf)

	// The broken backend is discarded and a fresh socket is dialed.
	replacement := takeBackend(t, accepted)
	replacement.Close()
}

func listenBackend(t *testing.T) (string, <-chan net.Conn) {
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

func takeBackend(t *testing.T, accepted <-chan net.Conn) net.Conn {
	t.Helper()
	select {
	case c := <-accepted:
		c.SetDeadline(time.Now().Add(5 * time.Second))
		t.Cleanup(func() { c.Close() })
		return c
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for backend connection")
		return nil
	}
}

func serveProxy(t *testing.T, p *pool.Pool) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go handleConnection(c, p)
		}
	}()
	return ln.Addr().String()
}

func dialProxy(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c.SetDeadline(time.Now().Add(5 * time.Second))
	t.Cleanup(func() { c.Close() })
	return c
}

func writeStartup(t *testing.T, c net.Conn, user, database string) {
	t.Helper()
	if _, err := c.Write(pgwire.BuildStartupRaw(user, database, nil)); err != nil {
		t.Fatal(err)
	}
}

func writeQuery(t *testing.T, c net.Conn, sql string) {
	t.Helper()
	if err := pgwire.WriteMessage(c, 'Q', append([]byte(sql), 0)); err != nil {
		t.Fatal(err)
	}
}

func readHandshake(t *testing.T, c net.Conn) {
	t.Helper()
	var sawR, sawZ bool
	for !sawZ {
		typ, payload, err := pgwire.ReadPayload(c)
		if err != nil {
			t.Fatal(err)
		}
		switch typ {
		case 'R':
			sawR = true
			if len(payload) != 4 || binary.BigEndian.Uint32(payload) != 0 {
				t.Fatalf("auth payload %v", payload)
			}
		case 'Z':
			sawZ = true
			if len(payload) != 1 || payload[0] != 'I' {
				t.Fatalf("ready payload %v", payload)
			}
		}
	}
	if !sawR {
		t.Fatal("missing AuthenticationOk")
	}
}

func expectReady(t *testing.T, c net.Conn, status byte) {
	t.Helper()
	for {
		typ, payload, err := pgwire.ReadPayload(c)
		if err != nil {
			t.Fatal(err)
		}
		if typ == 'Z' {
			if len(payload) != 1 || payload[0] != status {
				t.Fatalf("ReadyForQuery %v, want %q", payload, status)
			}
			return
		}
	}
}

func writeAuthOK(c net.Conn) error {
	if err := pgwire.WriteMessage(c, 'R', []byte{0, 0, 0, 0}); err != nil {
		return err
	}
	ps := append([]byte("server_version"), 0)
	ps = append(ps, []byte("16.0")...)
	ps = append(ps, 0)
	if err := pgwire.WriteMessage(c, 'S', ps); err != nil {
		return err
	}
	key := make([]byte, 8)
	binary.BigEndian.PutUint32(key[0:4], 42)
	binary.BigEndian.PutUint32(key[4:8], 7)
	if err := pgwire.WriteMessage(c, 'K', key); err != nil {
		return err
	}
	return pgwire.WriteMessage(c, 'Z', []byte{'I'})
}

func writeResult(c net.Conn, status byte) error {
	if err := pgwire.WriteMessage(c, 'C', append([]byte("SELECT 1"), 0)); err != nil {
		return err
	}
	return pgwire.WriteMessage(c, 'Z', []byte{status})
}

func pumpBackend(c net.Conn, id string, events chan<- string) {
	started := false
	for {
		kind, payload, err := readPacket(c)
		if err != nil {
			return
		}
		events <- id + " " + kind + " " + payload
		switch kind {
		case "startup":
			if started {
				return
			}
			started = true
			if err := writeAuthOK(c); err != nil {
				return
			}
		case "Q":
			if err := writeResult(c, 'I'); err != nil {
				return
			}
		}
	}
}

func recvEvent(t *testing.T, events <-chan string) string {
	t.Helper()
	select {
	case ev := <-events:
		return ev
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for backend event")
		return ""
	}
}

func splitEvent(ev string) (id, kind, payload string) {
	parts := strings.SplitN(ev, " ", 3)
	if len(parts) < 2 {
		return ev, "", ""
	}
	payload = ""
	if len(parts) == 3 {
		payload = parts[2]
	}
	return parts[0], parts[1], payload
}

// readPacket reads either a startup packet (no type byte; length MSB is 0
// for any message under 16MB) or one typed frontend message.
func readPacket(c net.Conn) (kind string, payload string, err error) {
	var first [1]byte
	if _, err = io.ReadFull(c, first[:]); err != nil {
		return "", "", err
	}
	if first[0] == 0 {
		var rest [3]byte
		if _, err = io.ReadFull(c, rest[:]); err != nil {
			return "", "", err
		}
		n := binary.BigEndian.Uint32([]byte{first[0], rest[0], rest[1], rest[2]})
		if n < 4 {
			return "", "", io.ErrUnexpectedEOF
		}
		body := make([]byte, n-4)
		if _, err = io.ReadFull(c, body); err != nil {
			return "", "", err
		}
		return "startup", "", nil
	}
	var lenBuf [4]byte
	if _, err = io.ReadFull(c, lenBuf[:]); err != nil {
		return "", "", err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n < 4 {
		return "", "", io.ErrUnexpectedEOF
	}
	body := make([]byte, n-4)
	if n > 4 {
		if _, err = io.ReadFull(c, body); err != nil {
			return "", "", err
		}
	}
	return string(first[:]), string(body), nil
}
