package ccdb

import (
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"iter"
	"unsafe"
)

type levelSnapshot struct {
	ss *leveldb.Snapshot
}

func (s levelSnapshot) Iterate(prefix string) iter.Seq[string] {
	it := s.ss.NewIterator(util.BytesPrefix(*(*[]byte)(unsafe.Pointer(&prefix))), nil)
	return func(yield func(string) bool) {
		defer it.Release()
		for it.Next() {
			key := it.Key()
			if !yield(string(key)) {
				return
			}
		}
	}
}

func (s levelSnapshot) Release() {
	s.ss.Release()
}

func (s levelSnapshot) Lookup(k string) (string, error) {
	val, err := s.ss.Get(*(*[]byte)(unsafe.Pointer(&k)), nil)
	if err != nil && err == leveldb.ErrNotFound {
		return "", nil
	}
	return string(val), err
}

type level struct {
	db *leveldb.DB
}

func (l level) Snapshot() (KVSnapshot, error) {
	ss, err := l.db.GetSnapshot()
	if err != nil {
		return nil, err
	}
	return levelSnapshot{ss: ss}, nil

}

func (l level) WriteChanges(ss *Snapshot) error {

	batch := &leveldb.Batch{}

	if ss.Readonly() {
		panic(errReadonly)
	}

	for k, v := range ss.Changes() {
		if len(v) == 0 {
			batch.Delete(*(*[]byte)(unsafe.Pointer(&k)))
		} else {
			batch.Put(*(*[]byte)(unsafe.Pointer(&k)), *(*[]byte)(unsafe.Pointer(&v)))
		}
	}

	if batch.Len() == 0 {
		panic(errors.New("empty write batch"))
	}

	return l.db.Write(batch, &opt.WriteOptions{Sync: true})

}

func LevelDB(database *leveldb.DB) KeyValueStore {
	return level{database}
}
