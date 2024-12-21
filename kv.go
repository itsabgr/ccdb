package ccdb

type KVSnapshot interface {
	Lookup(key string) (value string, err error)
	Release()
}

type KeyValueStore interface {
	Snapshot() (KVSnapshot, error)
	WriteChanges(ss *Snapshot) error
}
