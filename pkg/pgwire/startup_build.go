package pgwire

import (
	"encoding/binary"
)

// ProtocolVersion30 is PostgreSQL protocol 3.0: (3 << 16) | 0.
const ProtocolVersion30 uint32 = 196608

// BuildStartupRaw builds a StartupMessage suitable for dialing the backend
// with fixed pool credentials (user, database, optional extra params).
func BuildStartupRaw(user, database string, extra map[string]string) []byte {
	// body: protocol + key\0value\0 ... \0
	body := make([]byte, 0, 128)
	var ver [4]byte
	binary.BigEndian.PutUint32(ver[:], ProtocolVersion30)
	body = append(body, ver[:]...)

	put := func(k, v string) {
		body = append(body, k...)
		body = append(body, 0)
		body = append(body, v...)
		body = append(body, 0)
	}
	put("user", user)
	if database != "" {
		put("database", database)
	}
	for k, v := range extra {
		if k == "user" || k == "database" {
			continue
		}
		put(k, v)
	}
	body = append(body, 0) // terminator

	raw := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(raw[0:4], uint32(4+len(body)))
	copy(raw[4:], body)
	return raw
}
