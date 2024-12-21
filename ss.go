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

func (ss *Snapshot) Readonly() bool {
	return ss.version < 0
}

func (ss *Snapshot) Changes() iter.Seq2[string, string] {
	return ss.cc.Changes()
}

func (ss *Snapshot) NewTransaction() *Transaction {
	tx := ss.cc.UnsafeNoCopyNew()
	return &Transaction{tx, ss}
}

func (ss *Snapshot) iterate(prefix string) iter.Seq[string] {
	return ss.ss.Iterate(prefix)
}

func (ss *Snapshot) Commit(tx *Transaction) bool {
	return ss.cc.Commit(&tx.tx)
}

func (ss *Snapshot) Release() {
	ss.ss.Release()
}
