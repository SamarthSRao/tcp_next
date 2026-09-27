package pgwire

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestReadStartupPhase(t *testing.T) {
	raw := BuildStartupRaw("alice", "appdb", map[string]string{"application_name": "demo"})
	msg, err := ReadStartupPhase(bufRW{bytes.NewReader(raw), io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	if msg.User() != "alice" || msg.Database() != "appdb" {
		t.Fatalf("user=%q database=%q", msg.User(), msg.Database())
	}
	if msg.Params["application_name"] != "demo" {
		t.Fatalf("params %v", msg.Params)
	}
}

func TestReadStartupPhaseRejectsSSLThenParsesStartup(t *testing.T) {
	var in bytes.Buffer
	in.Write(sslRequestBytes())
	startup := BuildStartupRaw("bob", "db", nil)
	in.Write(startup)

	var out bytes.Buffer
	msg, err := ReadStartupPhase(bufRW{&in, &out})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), []byte{'N'}) {
		t.Fatalf("SSL reply %q", out.Bytes())
	}
	if msg.User() != "bob" || msg.Database() != "db" {
		t.Fatalf("parsed %+v", msg.Params)
	}
}

func TestReadStartupPhaseCancel(t *testing.T) {
	_, err := ReadStartupPhase(bufRW{bytes.NewReader(cancelRequestBytes()), io.Discard})
	if err == nil || !strings.Contains(err.Error(), "cancel") {
		t.Fatalf("got %v", err)
	}
}

func TestReadStartupPhaseLengthTooSmall(t *testing.T) {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], 7)
	_, err := ReadStartupPhase(bufRW{bytes.NewReader(lenBuf[:]), io.Discard})
	if err == nil || !strings.Contains(err.Error(), "too small") {
		t.Fatalf("got %v", err)
	}
}

func TestReadMessageRejectsShortLength(t *testing.T) {
	_, _, err := ReadMessage(bytes.NewReader([]byte{'Q', 0, 0, 0, 3}))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestWriteClientStartupOKReplaysFrames(t *testing.T) {
	param := frame(t, 'S', paramPayload("server_version", "16.0"))
	key := frame(t, 'K', keyPayload(42, 7))
	hs := &BackendHandshake{
		ParameterStatuses: [][]byte{param},
		BackendKeyData:    key,
	}
	var buf bytes.Buffer
	if err := WriteClientStartupOK(&buf, hs); err != nil {
		t.Fatal(err)
	}
	r := bytes.NewReader(buf.Bytes())

	typ, full, err := ReadMessage(r)
	if err != nil {
		t.Fatal(err)
	}
	if typ != 'R' || !bytes.Equal(full[5:], []byte{0, 0, 0, 0}) {
		t.Fatalf("auth frame %v %q", typ, full)
	}

	typ, full, err = ReadMessage(r)
	if err != nil {
		t.Fatal(err)
	}
	if typ != 'S' || !bytes.Equal(full, param) {
		t.Fatal("parameter status was not replayed unchanged")
	}

	typ, full, err = ReadMessage(r)
	if err != nil {
		t.Fatal(err)
	}
	if typ != 'K' || !bytes.Equal(full, key) {
		t.Fatal("backend key data was not replayed unchanged")
	}

	typ, full, err = ReadMessage(r)
	if err != nil {
		t.Fatal(err)
	}
	if typ != 'Z' || !bytes.Equal(full[5:], []byte{'I'}) {
		t.Fatalf("ready %v %q", typ, full)
	}
	if r.Len() != 0 {
		t.Fatalf("%d leftover bytes", r.Len())
	}
}

func TestWriteClientStartupOKSyntheticKey(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteClientStartupOK(&buf, &BackendHandshake{}); err != nil {
		t.Fatal(err)
	}
	r := bytes.NewReader(buf.Bytes())
	var key []byte
	for {
		typ, full, err := ReadMessage(r)
		if err != nil {
			t.Fatal(err)
		}
		if typ == 'K' {
			key = full[5:]
			break
		}
	}
	if len(key) != 8 {
		t.Fatalf("key %v", key)
	}
	if binary.BigEndian.Uint32(key[0:4]) != 1 || binary.BigEndian.Uint32(key[4:8]) != 1 {
		t.Fatalf("synthetic key pid/secret = %d/%d", binary.BigEndian.Uint32(key[0:4]), binary.BigEndian.Uint32(key[4:8]))
	}
}

func TestCompleteBackendStartupAuthOK(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	deadline := time.Now().Add(2 * time.Second)
	client.SetDeadline(deadline)
	server.SetDeadline(deadline)

	startup := BuildStartupRaw("alice", "db", nil)
	errCh := make(chan error, 1)
	hsCh := make(chan *BackendHandshake, 1)
	go func() {
		hs, err := CompleteBackendStartup(client, startup, "alice", "")
		if err != nil {
			errCh <- err
			return
		}
		hsCh <- hs
	}()

	if _, _, err := readStartupPacket(server); err != nil {
		t.Fatal(err)
	}
	// Notice and an unrecognized message are ignored.
	if err := WriteMessage(server, 'N', []byte{'M', 'h', 'i', 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := WriteMessage(server, 'v', []byte{0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	param := frame(t, 'S', paramPayload("server_version", "16.0"))
	key := frame(t, 'K', keyPayload(9, 8))
	if _, err := server.Write(param); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Write(key); err != nil {
		t.Fatal(err)
	}
	if err := WriteMessage(server, 'R', []byte{0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := WriteMessage(server, 'Z', []byte{'I'}); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errCh:
		t.Fatal(err)
	case hs := <-hsCh:
		if len(hs.ParameterStatuses) != 1 || !bytes.Equal(hs.ParameterStatuses[0], param) {
			t.Fatalf("params %v", hs.ParameterStatuses)
		}
		if !bytes.Equal(hs.BackendKeyData, key) {
			t.Fatal("missing key data")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

func TestCompleteBackendStartupCleartextPassword(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	deadline := time.Now().Add(2 * time.Second)
	client.SetDeadline(deadline)
	server.SetDeadline(deadline)

	errCh := make(chan error, 1)
	go func() {
		_, err := CompleteBackendStartup(client, BuildStartupRaw("alice", "db", nil), "alice", "s3cret")
		errCh <- err
	}()

	if _, _, err := readStartupPacket(server); err != nil {
		t.Fatal(err)
	}
	auth := make([]byte, 4)
	binary.BigEndian.PutUint32(auth, 3)
	if err := WriteMessage(server, 'R', auth); err != nil {
		t.Fatal(err)
	}
	typ, full, err := ReadMessage(server)
	if err != nil {
		t.Fatal(err)
	}
	if typ != 'p' || string(full[5:]) != "s3cret\x00" {
		t.Fatalf("password message %q %q", typ, full[5:])
	}
	if err := WriteMessage(server, 'R', []byte{0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := WriteMessage(server, 'Z', []byte{'I'}); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestCompleteBackendStartupMD5Password(t *testing.T) {
	// user=postgres password=secret salt=01020304
	// md5( hex(md5(password+user)) + salt ) with the "md5" prefix.
	const want = "md5bb41a296aab6baccb36ff243a562abff\x00"

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	deadline := time.Now().Add(2 * time.Second)
	client.SetDeadline(deadline)
	server.SetDeadline(deadline)

	errCh := make(chan error, 1)
	go func() {
		_, err := CompleteBackendStartup(client, BuildStartupRaw("postgres", "postgres", nil), "postgres", "secret")
		errCh <- err
	}()

	if _, _, err := readStartupPacket(server); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 8)
	binary.BigEndian.PutUint32(payload[0:4], 5)
	copy(payload[4:], []byte{1, 2, 3, 4})
	if err := WriteMessage(server, 'R', payload); err != nil {
		t.Fatal(err)
	}
	typ, full, err := ReadMessage(server)
	if err != nil {
		t.Fatal(err)
	}
	if typ != 'p' || string(full[5:]) != want {
		t.Fatalf("got %q want %q", full[5:], want)
	}
	if err := WriteMessage(server, 'R', []byte{0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := WriteMessage(server, 'Z', []byte{'I'}); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestCompleteBackendStartupMD5RequiresPassword(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	deadline := time.Now().Add(2 * time.Second)
	client.SetDeadline(deadline)
	server.SetDeadline(deadline)

	errCh := make(chan error, 1)
	go func() {
		_, err := CompleteBackendStartup(client, BuildStartupRaw("postgres", "postgres", nil), "postgres", "")
		errCh <- err
	}()
	if _, _, err := readStartupPacket(server); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 8)
	binary.BigEndian.PutUint32(payload[0:4], 5)
	copy(payload[4:], []byte{1, 2, 3, 4})
	if err := WriteMessage(server, 'R', payload); err != nil {
		t.Fatal(err)
	}
	err := <-errCh
	if err == nil || !strings.Contains(err.Error(), "PGPASSWORD") {
		t.Fatalf("got %v", err)
	}
}

func TestCompleteBackendStartupRejectsSASL(t *testing.T) {
	err := authTypeError(t, 10, nil)
	if err == nil || !strings.Contains(err.Error(), "SASL") {
		t.Fatalf("got %v", err)
	}
}

func TestCompleteBackendStartupUnsupportedAuth(t *testing.T) {
	err := authTypeError(t, 9, nil)
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("got %v", err)
	}
}

func TestCompleteBackendStartupErrorResponse(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	deadline := time.Now().Add(2 * time.Second)
	client.SetDeadline(deadline)
	server.SetDeadline(deadline)

	errCh := make(chan error, 1)
	go func() {
		_, err := CompleteBackendStartup(client, BuildStartupRaw("alice", "db", nil), "alice", "")
		errCh <- err
	}()
	if _, _, err := readStartupPacket(server); err != nil {
		t.Fatal(err)
	}
	var payload []byte
	add := func(tag byte, val string) {
		payload = append(payload, tag)
		payload = append(payload, val...)
		payload = append(payload, 0)
	}
	add('S', "ERROR")
	add('C', "28P01")
	add('M', "password authentication failed")
	payload = append(payload, 0)
	if err := WriteMessage(server, 'E', payload); err != nil {
		t.Fatal(err)
	}
	err := <-errCh
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "password authentication failed") || !strings.Contains(err.Error(), "28P01") {
		t.Fatalf("got %v", err)
	}
}

func authTypeError(t *testing.T, authType uint32, extra []byte) error {
	t.Helper()
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	deadline := time.Now().Add(2 * time.Second)
	client.SetDeadline(deadline)
	server.SetDeadline(deadline)

	errCh := make(chan error, 1)
	go func() {
		_, err := CompleteBackendStartup(client, BuildStartupRaw("alice", "db", nil), "alice", "pw")
		errCh <- err
	}()
	if _, _, err := readStartupPacket(server); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 4+len(extra))
	binary.BigEndian.PutUint32(payload[:4], authType)
	copy(payload[4:], extra)
	if err := WriteMessage(server, 'R', payload); err != nil {
		t.Fatal(err)
	}
	return <-errCh
}

func readStartupPacket(r io.Reader) (uint32, []byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	body := make([]byte, n-4)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, err
	}
	return binary.BigEndian.Uint32(body[:4]), body, nil
}

func sslRequestBytes() []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], 8)
	binary.BigEndian.PutUint32(buf[4:8], SSLRequestCode)
	return buf
}

func cancelRequestBytes() []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint32(buf[0:4], 16)
	binary.BigEndian.PutUint32(buf[4:8], CancelRequestCode)
	binary.BigEndian.PutUint32(buf[8:12], 1)
	binary.BigEndian.PutUint32(buf[12:16], 2)
	return buf
}

func frame(t *testing.T, msgType byte, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := WriteMessage(&buf, msgType, payload); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func paramPayload(key, value string) []byte {
	payload := append([]byte(key), 0)
	payload = append(payload, value...)
	payload = append(payload, 0)
	return payload
}

func keyPayload(pid, secret uint32) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], pid)
	binary.BigEndian.PutUint32(buf[4:8], secret)
	return buf
}
