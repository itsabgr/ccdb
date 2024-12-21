package ccdb

import (
	"iter"
	"sync"
)

type Snapshot struct {
	cc      concurrencyController[string, string]
	ss      KVSnapshot
	db      *Database
	version int64
	_       sync.Mutex
}

func (u *Snapshot) Readonly() bool {
	return u.version < 0
}

func (u *Snapshot) Changes() iter.Seq2[string, string] {
	return u.cc.Changes()
}

func (u *Snapshot) NewTransaction() *Transaction {
	tx := u.cc.UnsafeNoCopyNew()
	return &Transaction{tx}
}

func (u *Snapshot) Commit(tx *Transaction) bool {
	return u.cc.Commit(&tx.tx)
}

func (u *Snapshot) Release() {
	u.ss.Release()
}
