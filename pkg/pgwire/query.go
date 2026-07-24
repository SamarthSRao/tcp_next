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
	return Z
}
