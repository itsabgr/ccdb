package ccdb

import (
	"errors"
	"iter"
)

var errTSOverflow = errors.New("concurrency controller ts overflow")

type readResult[V any] struct {
	lookup bool
	cached bool
	err    error
	vv     vv[V]
}

type vv[V any] struct {
	value   V
	version int64
}

type txn[K comparable, V any] struct {
	ts     int64
	parent *concurrencyController[K, V]
	db     map[K]vv[V]
}

func (txn *txn[K, V]) Changes() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for k, v := range txn.db {
			if v.version > 0 {
				if !yield(k, v.value) {
					return
				}
			}
		}
	}
}

func (txn *txn[K, V]) Get(k K) (V, error) {
	res := &readResult[V]{}
	txn.Read(k, res)
	return res.vv.value, res.err
}

func (txn *txn[K, V]) Read(k K, result *readResult[V]) {
	v, ok := txn.db[k]
	if ok {
		result.lookup = false
		result.cached = true
		result.vv = v
		result.err = nil
		return
	}
	txn.parent.get(k, result)
	if result.err != nil {
		result.cached = false
		return
	}
	if v.version >= txn.ts {
		result.cached = false
		result.err = ErrModified{}
		return
	}
	txn.db[k] = v
	result.cached = true
	return
}

func (txn *txn[K, V]) Set(k K, v V) {
	txn.db[k] = vv[V]{v, -1}
}

type concurrencyController[K comparable, V any] struct {
	time   int64
	lookup func(k K) (V, error)
	db     map[K]vv[V]
}

func (cc *concurrencyController[K, V]) UnsafeNoCopyNew() txn[K, V] {
	cc.time++
	ts := cc.time
	if ts <= 0 {
		cc.time--
		panic(errVersionOverflow)
	}
	return txn[K, V]{
		ts:     ts,
		parent: cc,
		db:     make(map[K]vv[V]),
	}
}

func (cc *concurrencyController[K, V]) Changes() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for k, v := range cc.db {
			if v.version > 0 {
				if !yield(k, v.value) {
					return
				}
			}
		}
	}
}

func (cc *concurrencyController[K, V]) Commit(txn *txn[K, V]) bool {

	for k := range txn.db {
		if cc.db[k].version >= txn.ts {
			return false
		}

	}

	cc.time++
	ts := cc.time

	if ts <= 0 {
		cc.time--
		panic(errTSOverflow)
	}

	for k, v := range txn.db {
		if v.version == -1 {
			v.version = ts
			cc.db[k] = v
		}
	}

	return true
}

func (cc *concurrencyController[K, V]) get(k K, result *readResult[V]) {
	v, ok := cc.db[k]
	if ok {
		result.lookup = false
		result.vv = v
		result.err = nil
		return
	}
	value, err := cc.lookup(k)
	if err != nil {
		result.lookup = true
		result.vv.value = value
		result.vv.version = 0
		result.err = err
		return
	}
	v = vv[V]{value, 0}
	cc.db[k] = v

	result.lookup = true
	result.vv = v
	result.err = nil
	return

}
