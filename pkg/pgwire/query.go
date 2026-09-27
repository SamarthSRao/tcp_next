package pgwire

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
)

func ReadPayload(r io.Reader) (msgType byte, payload []byte, err error) {
	header := make([]byte, 5)
	_, err = io.ReadFull(r, header)
	if err != nil {
		return 0, nil, err
	}

	msgType = header[0]
	length := binary.BigEndian.Uint32(header[1:])
	if length < 4 {
		return 0, nil, fmt.Errorf("invalid message length %d", length)
	}
	payload = make([]byte, length-4)
	if len(payload) > 0 {
		_, err = io.ReadFull(r, payload)
		if err != nil {
			return 0, nil, err
		}
	}
	return msgType, payload, nil

}

func ReadyForQueryStatus(Z byte) []byte {

	// PostgreSQL ReadyForQuery transaction-status bytes.
	const Idle byte = 'I'
	const InTransaction byte = 'T'
	const FailedTransaction byte = 'E'
	if Z == Idle {
		return []byte("idle")
	}
	if Z == InTransaction {
		return []byte("in_transaction")
	}
	if Z == FailedTransaction {
		return []byte("failed_transaction")
	}
	return []byte{Z}
}

func ClassifyQuery(message string) string {
	message = strings.TrimSpace(message)
	message = strings.ToUpper(message)
	message = strings.Trim(message, " \t\n\r;")
	if strings.HasPrefix(message, "BEGIN") {
		return "begin"
	}
	if strings.HasPrefix(message, "COMMIT") {
		return "commit"
	}
	if strings.HasPrefix(message, "ROLLBACK") {
		return "rollback"
	}
	return "unknown"
}
