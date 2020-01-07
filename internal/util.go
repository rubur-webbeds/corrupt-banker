package util

import "log"

type Transaction struct {
	Action   string
	Amount   int
	ClientId string
}

type TransactionResult struct {
	Action   string
	Amount   int
	ClientId string
	Ok       bool
	Message  string
}

func FailOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
