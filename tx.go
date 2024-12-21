package ccdb

import (
	"errors"
	"iter"
)

type Transaction struct {
	tx txn[string, string]
}

func (tx *Transaction) Get(k string) (string, error) {
	return tx.tx.Get(k)
}

func (tx *Transaction) Set(key, val string) {
	if len(key) == 0 {
		panic(errors.New("empty key"))
	}
	tx.tx.Set(key, val)
}

func (tx *Transaction) Changes() iter.Seq2[string, string] {
	return tx.tx.Changes()
}
