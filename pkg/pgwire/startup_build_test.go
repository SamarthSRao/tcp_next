package pgwire

import (
	"bytes"
	"io"
	"testing"
)

type bufRW struct {
	r io.Reader
	w io.Writer
}

func (b bufRW) Read(p []byte) (int, error)  { return b.r.Read(p) }
func (b bufRW) Write(p []byte) (int, error) { return b.w.Write(p) }

func TestBuildStartupRawRoundTrip(t *testing.T) {
	raw := BuildStartupRaw("alice", "appdb", map[string]string{
		"application_name": "demo",
		"user":             "ignored",
		"database":         "ignored",
	})
	if len(raw) < 8 {
		t.Fatalf("raw too short: %d", len(raw))
	}
	if int(raw[0])<<24|int(raw[1])<<16|int(raw[2])<<8|int(raw[3]) != len(raw) {
		t.Fatalf("length prefix %d, raw len %d", raw[:4], len(raw))
	}

	msg, err := ReadStartupPhase(bufRW{bytes.NewReader(raw), io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	if msg.ProtocolVersion != ProtocolVersion30 {
		t.Fatalf("protocol %d", msg.ProtocolVersion)
	}
	if msg.User() != "alice" {
		t.Fatalf("user %q", msg.User())
	}
	if msg.Database() != "appdb" {
		t.Fatalf("database %q", msg.Database())
	}
	if msg.Params["application_name"] != "demo" {
		t.Fatalf("params %v", msg.Params)
	}
	if !bytes.Equal(msg.Raw, raw) {
		t.Fatal("Raw was not the original startup bytes")
	}
}

func TestBuildStartupRawOmitsEmptyDatabase(t *testing.T) {
	raw := BuildStartupRaw("alice", "", nil)
	msg, err := ReadStartupPhase(bufRW{bytes.NewReader(raw), io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := msg.Params["database"]; ok {
		t.Fatalf("database should be omitted, params %v", msg.Params)
	}
	if msg.User() != "alice" {
		t.Fatalf("user %q", msg.User())
	}
}
