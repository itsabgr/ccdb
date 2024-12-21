package ccdb

import "iter"

type KVSnapshot interface {
	Lookup(key string) (value string, err error)
	Iterate(prefix string) iter.Seq[string]
	Release()
}

type KeyValueStore interface {
	Snapshot() (KVSnapshot, error)
	WriteChanges(ss *Snapshot) error
}
