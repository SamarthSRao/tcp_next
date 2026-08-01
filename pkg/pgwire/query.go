package pgwire

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
func HandleMessage(message string) string {
	if message == "BEGIN" {
		return "begin"
	}
	if message == "COMMIT" {
		return "commit"
	}
	if message == "ROLLBACK" {
		return "rollback"
	}
	return "unknown"
}
