package ccdb

import (
	"errors"
	"math"
	"sync/atomic"
)

type Database struct {
	version atomic.Int64
	storage KeyValueStore
}

var errVersionOverflow = errors.New("db version overflow")

func New(storage KeyValueStore) *Database {

	return &Database{
		storage: storage,
	}

}

func (db *Database) Snapshot(readonly bool) (*Snapshot, error) {
	ss, err := db.storage.Snapshot()

	if err != nil {
		return nil, err
	}

	close_ := true

	defer func() {
		if close_ {
			ss.Release()
		}
	}()

	version := int64(-1)

	if !readonly {
		if version = db.version.Load(); version < 0 || version >= math.MaxInt64 {
			panic(errVersionOverflow)
		}
	}

	cc := concurrencyController[string, string]{
		time:   0,
		lookup: ss.Lookup,
		db:     make(map[string]vv[string]),
	}

	close_ = false

	return &Snapshot{
		cc:      cc,
		ss:      ss,
		db:      db,
		version: version,
	}, nil

}

var errReadonly = errors.New("snapshot is readonly")

func (db *Database) WriteChanges(ss *Snapshot) error {

	if ss.db != db {
		panic(errors.New("foreign snapshot"))
	}

	if ss.Readonly() {
		panic(errReadonly)
	}

	if ss.version < 0 || ss.version >= math.MaxInt64 {
		panic(errVersionOverflow)
	}

	if false == db.version.CompareAndSwap(ss.version, ss.version+1) {
		return ErrModified{}
	}

	return db.storage.WriteChanges(ss)

}
