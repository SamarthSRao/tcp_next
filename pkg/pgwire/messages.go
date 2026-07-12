package pgwire

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
)

// SSLRequestCode is the protocol code clients send before StartupMessage when
// asking whether the server supports SSL.
const SSLRequestCode uint32 = 80877103

// CancelRequestCode is used for query cancellation (not handled as a session).
const CancelRequestCode uint32 = 80877102

// StartupMessage is the first real session message from a frontend.
type StartupMessage struct {
	ProtocolVersion uint32
	Params          map[string]string
	// Raw is length(4) + body so it can be forwarded to a backend unchanged.
	Raw []byte
}

// User returns the "user" startup parameter (empty if missing).
func (s *StartupMessage) User() string { return s.Params["user"] }

// Database returns the "database" startup parameter (empty if missing).
func (s *StartupMessage) Database() string { return s.Params["database"] }

// ReadStartupPhase handles optional SSLRequest, then reads and parses StartupMessage.
// On SSLRequest it writes 'N' (no SSL) and continues. Cancel requests return an error.
func ReadStartupPhase(client io.ReadWriter) (*StartupMessage, error) {
	for {
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(client, lenBuf); err != nil {
			return nil, fmt.Errorf("read startup length: %w", err)
		}
		totalLen := binary.BigEndian.Uint32(lenBuf)
		if totalLen < 8 {
			return nil, fmt.Errorf("startup length too small: %d", totalLen)
		}

		body := make([]byte, totalLen-4)
		if _, err := io.ReadFull(client, body); err != nil {
			return nil, fmt.Errorf("read startup body: %w", err)
		}

		code := binary.BigEndian.Uint32(body[0:4])

		// SSLRequest: length is always 8 (4 + 4). Reply 'N' and wait for real startup.
		if totalLen == 8 && code == SSLRequestCode {
			if _, err := client.Write([]byte{'N'}); err != nil {
				return nil, fmt.Errorf("write SSL rejection: %w", err)
			}
			continue
		}

		if totalLen == 16 && code == CancelRequestCode {
			return nil, fmt.Errorf("cancel request received (not a normal session)")
		}

		// Real StartupMessage: protocol version + key\0value\0 ... \0
		msg := &StartupMessage{
			ProtocolVersion: code,
			Params:          parseStartupParams(body[4:]),
			Raw:             append(append([]byte{}, lenBuf...), body...),
		}
		return msg, nil
	}
}

func parseStartupParams(payload []byte) map[string]string {
	params := make(map[string]string)
	// payload is key\0value\0key\0value\0\0
	parts := strings.Split(string(payload), "\x00")
	for i := 0; i+1 < len(parts); i += 2 {
		key := parts[i]
		if key == "" {
			break
		}
		params[key] = parts[i+1]
	}
	return params
}

// BackendHandshake holds messages collected while completing backend auth.
type BackendHandshake struct {
	ParameterStatuses [][]byte // full 'S' frames (type + length + payload)
	BackendKeyData    []byte   // full 'K' frame
}

// CompleteBackendStartup writes the client startup (or equivalent) to the backend
// and reads until ReadyForQuery. AuthenticationOk from the backend is consumed
// here so the proxy can spoof auth toward the client.
//
// password is used if the backend asks for cleartext or MD5 auth.
// user is required for MD5 challenge response.
func CompleteBackendStartup(backend io.ReadWriter, startupRaw []byte, user, password string) (*BackendHandshake, error) {
	if _, err := backend.Write(startupRaw); err != nil {
		return nil, fmt.Errorf("write startup to backend: %w", err)
	}

	hs := &BackendHandshake{}
	for {
		msgType, frame, err := ReadMessage(backend)
		if err != nil {
			return nil, fmt.Errorf("read backend startup message: %w", err)
		}

		switch msgType {
		case 'R': // Authentication*
			if err := handleBackendAuth(backend, frame, user, password); err != nil {
				return nil, err
			}
		case 'S': // ParameterStatus — keep full frame for client replay
			hs.ParameterStatuses = append(hs.ParameterStatuses, frame)
		case 'K': // BackendKeyData
			hs.BackendKeyData = frame
		case 'Z': // ReadyForQuery — backend session is live
			return hs, nil
		case 'E': // ErrorResponse
			return nil, fmt.Errorf("backend startup error: %s", formatErrorResponse(frame))
		case 'N': // NoticeResponse — ignore during startup
			continue
		default:
			// e.g. NegotiateProtocolVersion — ignore for now
			continue
		}
	}
}

func handleBackendAuth(backend io.ReadWriter, frame []byte, user, password string) error {
	// frame: 'R' + int32 len + int32 authType + optional payload
	if len(frame) < 9 {
		return fmt.Errorf("auth message too short")
	}
	authType := binary.BigEndian.Uint32(frame[5:9])
	switch authType {
	case 0: // AuthenticationOk
		return nil
	case 3: // Cleartext password
		return writePasswordMessage(backend, password)
	case 5: // MD5 password: 4-byte salt follows auth type
		if len(frame) < 13 {
			return fmt.Errorf("MD5 auth missing salt")
		}
		if password == "" {
			return fmt.Errorf("MD5 auth requested; set PGPASSWORD")
		}
		salt := frame[9:13]
		return writePasswordMessage(backend, md5Password(user, password, salt))
	case 10: // SASL (SCRAM)
		return fmt.Errorf("backend requested SASL/SCRAM; for local dev use trust/md5 in pg_hba.conf or set a non-SCRAM user (SCRAM comes later)")
	default:
		return fmt.Errorf("unsupported backend auth type %d", authType)
	}
}

// md5Password builds the PostgreSQL MD5 auth string:
// "md5" + hex( md5( hex(md5(password+user)) + salt ) )
func md5Password(user, password string, salt []byte) string {
	inner := md5.Sum([]byte(password + user))
	innerHex := hex.EncodeToString(inner[:])
	outer := md5.Sum(append([]byte(innerHex), salt...))
	return "md5" + hex.EncodeToString(outer[:])
}

func writePasswordMessage(w io.Writer, password string) error {
	// PasswordMessage (F): 'p' + int32 len + string password + '\0'
	payload := append([]byte(password), 0)
	return WriteMessage(w, 'p', payload)
}

// WriteClientStartupOK spoofs a successful server handshake toward the client:
// AuthenticationOk, ParameterStatus(es), BackendKeyData, ReadyForQuery.
func WriteClientStartupOK(client io.Writer, hs *BackendHandshake) error {
	// AuthenticationOk: 'R' + len=8 + authType=0
	if err := WriteMessage(client, 'R', []byte{0, 0, 0, 0}); err != nil {
		return err
	}
	for _, s := range hs.ParameterStatuses {
		if _, err := client.Write(s); err != nil {
			return err
		}
	}
	if len(hs.BackendKeyData) > 0 {
		if _, err := client.Write(hs.BackendKeyData); err != nil {
			return err
		}
	} else {
		// Synthetic key data if backend omitted it (should not happen).
		body := make([]byte, 8)
		binary.BigEndian.PutUint32(body[0:4], 1) // pid
		binary.BigEndian.PutUint32(body[4:8], 1) // secret
		if err := WriteMessage(client, 'K', body); err != nil {
			return err
		}
	}
	// ReadyForQuery idle: 'Z' + len=5 + 'I'
	return WriteMessage(client, 'Z', []byte{'I'})
}

// ReadMessage reads one typed backend/frontend message: type(1) + len(4) + payload.
// Returns the type and the full frame (type+len+payload).
func ReadMessage(r io.Reader) (byte, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, nil, err
	}
	msgLen := binary.BigEndian.Uint32(header[1:5])
	if msgLen < 4 {
		return 0, nil, fmt.Errorf("invalid message length %d", msgLen)
	}
	payloadLen := msgLen - 4
	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return 0, nil, err
		}
	}
	frame := append(append([]byte{}, header...), payload...)
	return header[0], frame, nil
}

// WriteMessage writes type + length (4 + len(payload)) + payload.
func WriteMessage(w io.Writer, msgType byte, payload []byte) error {
	buf := make([]byte, 5+len(payload))
	buf[0] = msgType
	binary.BigEndian.PutUint32(buf[1:5], uint32(4+len(payload)))
	copy(buf[5:], payload)
	_, err := w.Write(buf)
	return err
}

func formatErrorResponse(frame []byte) string {
	// 'E' + len + fields: type byte + string\0 ... terminated by \0
	if len(frame) < 6 {
		return "unknown error"
	}
	var msg, code string
	i := 5
	for i < len(frame) {
		if frame[i] == 0 {
			break
		}
		ft := frame[i]
		i++
		start := i
		for i < len(frame) && frame[i] != 0 {
			i++
		}
		val := string(frame[start:i])
		if i < len(frame) {
			i++ // skip NUL
		}
		switch ft {
		case 'M':
			msg = val
		case 'C':
			code = val
		}
	}
	if code != "" {
		return fmt.Sprintf("%s (%s)", msg, code)
	}
	return msg
}
