package main

import (
	"fmt"
	"github.com/itsabgr/ccdb"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"iter"
)

func main() {
	leveldbStorage := storage.NewMemStorage()

	defer leveldbStorage.Close()

	level, err := leveldb.Open(leveldbStorage, nil)
	if err != nil {
		panic(err)
	}

	defer level.Close()

	kvStore := ccdb.LevelDB(level)

	db := ccdb.New(kvStore)

	ss, err := db.Snapshot(false)
	if err != nil {
		panic(err)
	}
	defer ss.Release()

	tx := ss.NewTransaction()

	tx.Set("foo", "bar")

	fmt.Println(tx.Get("foo"))

	fmt.Println(ss.Commit(tx))

	if empty(ss.Changes()) {
		return
	}

	fmt.Println(db.WriteChanges(ss))

}

func empty[K any, V any](it iter.Seq2[K, V]) bool {
	for range it {
		return false
	}
	return true
}
