package pgwire

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestReadPayloadRoundTrip(t *testing.T) {
	payload := append([]byte("SELECT 1"), 0)
	var buf bytes.Buffer
	if err := WriteMessage(&buf, 'Q', payload); err != nil {
		t.Fatal(err)
	}

	msgType, got, err := ReadPayload(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if msgType != 'Q' {
		t.Fatalf("type %q", msgType)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload %q", got)
	}
}

func TestReadPayloadEmptyBody(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteMessage(&buf, 'S', nil); err != nil {
		t.Fatal(err)
	}
	// Sync has an empty payload; length is 4.
	msgType, got, err := ReadPayload(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if msgType != 'S' || len(got) != 0 {
		t.Fatalf("type %q payload %v", msgType, got)
	}
}

func TestReadPayloadRejectsShortLength(t *testing.T) {
	// Length 3 is smaller than the 4-byte length field itself.
	// A uint32 subtraction would wrap and allocate a huge slice.
	buf := bytes.NewReader([]byte{'Q', 0, 0, 0, 3})
	_, _, err := ReadPayload(buf)
	if err == nil {
		t.Fatal("expected error")
	}

	buf = bytes.NewReader([]byte{'Q', 0, 0, 0, 0})
	_, _, err = ReadPayload(buf)
	if err == nil {
		t.Fatal("expected error for length 0")
	}
}

func TestReadPayloadUnexpectedEOF(t *testing.T) {
	// Header claims 10 payload bytes; only 2 follow.
	raw := []byte{'Q', 0, 0, 0, 14, 'a', 'b'}
	_, _, err := ReadPayload(bytes.NewReader(raw))
	if !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		t.Fatalf("got %v", err)
	}
}

func TestReadyForQueryStatus(t *testing.T) {
	tests := []struct {
		status byte
		want   string
	}{
		{'I', "idle"},
		{'T', "in_transaction"},
		{'E', "failed_transaction"},
		{'Z', "Z"},
	}
	for _, tt := range tests {
		got := string(ReadyForQueryStatus(tt.status))
		if got != tt.want {
			t.Errorf("status %q: got %q want %q", tt.status, got, tt.want)
		}
	}
}

func TestClassifyQuery(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"BEGIN", "begin"},
		{"begin transaction", "begin"},
		{"  Commit; ", "commit"},
		{"ROLLBACK WORK", "rollback"},
		{";;rollback;;", "rollback"},
		{"SELECT 1", "unknown"},
		{"", "unknown"},
		// Prefix match, not a keyword tokenizer.
		{"BEGINNING", "begin"},
	}
	for _, tt := range tests {
		got := ClassifyQuery(tt.in)
		if got != tt.want {
			t.Errorf("ClassifyQuery(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
