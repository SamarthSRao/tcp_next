package pgwire

import (
	"encoding/binary"
	"io"
	"strings"
)

func ReadMessage(r io.Reader) (msgType byte, payload []byte, err error) {
	header := make([]byte, 5)
	_, err = io.ReadFull(r, header)
	if err != nil {
		return 0, nil, err
	}

	msgType = header[0]
	length := binary.BigEndian.Uint32(header[1:])
	payload = make([]byte, length-4)
	_, err = io.ReadFull(r, payload)
	if err != nil {
		return 0, nil, err
	}
	return msgType, payload, nil

}

func ReadyForQueryStatus(Z byte) []byte {

	const Idle byte = 'I'
	const InTransaction byte = 'B'
	const FailedTransaction byte = 'D'
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
