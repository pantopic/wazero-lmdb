package main

import (
	"encoding/binary"
	"errors"
	"unsafe"

	"github.com/pantopic/wazero-lmdb/lmdb-go"
)

func main() {}

var txn *lmdb.Txn
var cur *lmdb.Cursor
var dbi lmdb.DBI
var err error
var k = make([]byte, 16)
var v = make([]byte, 16)

//export begin
func begin() {
	txn, err = lmdb.BeginTxn(nil, 0)
	if err != nil {
		panic(err)
	}
}

//export beginread
func beginread() {
	txn, err = lmdb.BeginTxn(nil, lmdb.Readonly)
	if err != nil {
		panic(err)
	}
}

//export db
func db() {
	dbi, err = txn.OpenDBI("test", lmdb.Create)
	if err != nil {
		panic(err)
	}
}

//export dbstat
func dbstat() uint64 {
	s, err := txn.Stat(dbi)
	if err != nil {
		panic(err)
	}
	return sliceToPtr(s.ToBytes())
}

//export dbdrop
func dbdrop() {
	err := txn.Drop(dbi)
	if err != nil {
		panic(err)
	}
}

//export set
func set() {
	err := txn.Put(dbi, []byte(`a`), []byte(`1`), 0)
	if err != nil {
		panic(err)
	}
}

//export get
func get() {
	v, err := txn.Get(dbi, []byte(`a`))
	if err != nil {
		panic(err)
	}
	if string(v) != `1` {
		panic(`wrong value`)
	}
}

//export getmissing
func getmissing() {
	_, err := txn.Get(dbi, []byte(`ddd`))
	if err == nil {
		panic(`error not returned`)
	}
	if !lmdb.IsNotFound(err) {
		panic(err)
	}
}

//export del
func del() {
	err := txn.Del(dbi, []byte(`a`), nil)
	if err != nil {
		panic(err)
	}
}

//export commit
func commit() {
	err := txn.Commit()
	if err != nil {
		panic(err)
	}
}

//export set2
func set2() {
	err := txn.Put(dbi, []byte(`b`), []byte(`2`), 0)
	if err != nil {
		panic(err)
	}
}

//export get2
func get2() {
	v, err := txn.Get(dbi, []byte(`b`))
	if err != nil {
		panic(err)
	}
	if string(v) != `2` {
		panic(`wrong value`)
	}
}

//export update
func update() {
	if err := lmdb.Update(func(txn *lmdb.Txn) error {
		txn.Put(dbi, []byte(`b`), []byte(`22`), 0)
		return nil
	}); err != nil {
		panic(err)
	}
}

//export updatefail
func updatefail() {
	if err := lmdb.Update(func(txn *lmdb.Txn) error {
		txn.Put(dbi, []byte(`b`), []byte(`222`), 0)
		return errors.New(`I can't believe you've done this.`)
	}); err == nil {
		panic(`Error missing`)
	}
}

//export view
func view() {
	err := lmdb.View(func(txn *lmdb.Txn) (err error) {
		v, err := txn.Get(dbi, []byte(`b`))
		if err != nil {
			return
		}
		if string(v) != `22` {
			err = errors.New("Wrong value: " + string(v) + " != 22")
		}
		return
	})
	if err != nil {
		panic(err)
	}
}

//export clear
func clear() {
	if err := lmdb.Update(func(txn *lmdb.Txn) error {
		dbi, err = txn.OpenDBI("test", lmdb.Create)
		if err != nil {
			panic(err)
		}
		return txn.Drop(dbi)
	}); err != nil {
		panic(err)
	}
}

//export sub
func sub() {
	if err := lmdb.Update(func(txn *lmdb.Txn) error {
		return txn.Sub(func(txn *lmdb.Txn) error {
			return txn.Put(dbi, []byte(`sub`), []byte(`txn`), 0)
		})
	}); err != nil {
		panic(err)
	}
}

//export subabort
func subabort() {
	if err := lmdb.Update(func(txn *lmdb.Txn) error {
		txn.Sub(func(txn *lmdb.Txn) error {
			txn.Put(dbi, []byte(`sub`), []byte(`txn`), 0)
			return errors.New(`I can't believe you've done this.`)
		})
		return nil
	}); err != nil {
		panic(err)
	}
}

//export subdel
func subdel() {
	if err := lmdb.Update(func(txn *lmdb.Txn) error {
		return txn.Sub(func(txn *lmdb.Txn) error {
			return txn.Del(dbi, []byte(`sub`), nil)
		})
	}); err != nil {
		panic(err)
	}
}

//export stress
func stress(limit uint32) {
	txn, err = lmdb.BeginTxn(nil, 0)
	if err != nil {
		panic(err)
	}
	if dbi, err = txn.OpenDBI("test", lmdb.Create); err != nil {
		panic(err)
	}
	n := uint64(limit)
	for i := range n {
		binary.LittleEndian.PutUint64(k, i+1e15)
		binary.LittleEndian.PutUint64(v, n-i+1e15)
		if err := txn.Put(dbi, k, v, 0); err != nil {
			panic(err)
		}
	}
	err := txn.Commit()
	if err != nil {
		panic(err)
	}
}

//export abort
func abort() {
	txn.Abort()
}

//export cursoropen
func cursoropen() {
	if dbi, err = txn.OpenDBI("test", lmdb.Create); err != nil {
		panic(err)
	}
	if cur, err = txn.OpenCursor(dbi); err != nil {
		panic(err)
	}
}

//export cursorfirst
func cursorfirst() {
	k, v, err := cur.Get(nil, nil, lmdb.First)
	if err != nil {
		panic(err)
	}
	if string(k) != `b` {
		panic(`wrong key: ` + string(k))
	}
	if string(v) != `22` {
		panic(`wrong value: ` + string(v))
	}
}

//export cursorput
func cursorput() {
	err := cur.Put([]byte(`c`), []byte(`3`), 0)
	if err != nil {
		panic(err)
	}
}

//export cursorcurrent
func cursorcurrent() {
	k, v, err := cur.Get(nil, nil, lmdb.GetCurrent)
	if err != nil {
		panic(err)
	}
	if string(k) != `c` {
		panic(`wrong key: ` + string(k))
	}
	if string(v) != `3` {
		panic(`wrong value: ` + string(v))
	}
}

//export cursornext
func cursornext() {
	k, v, err := cur.Get(nil, nil, lmdb.Next)
	if err != nil {
		panic(err)
	}
	if string(k) != `c` {
		panic(`wrong key: ` + string(k))
	}
	if string(v) != `3` {
		panic(`wrong value: ` + string(v))
	}
}

//export cursordel
func cursordel() {
	err := cur.Del(lmdb.Current)
	if err != nil {
		panic(err)
	}
}

//export cursorclose
func cursorclose() {
	cur.Close()
}

//export valptrs2
func valptrs2() uint64 {
	return uint64(uintptr(unsafe.Pointer(&k[0])))<<32 + uint64(uintptr(unsafe.Pointer(&v[0])))
}

//export setval
func setval() {
	if err := txn.Put(dbi, k, v, 0); err != nil {
		panic(err)
	}
}

func sliceToPtr(b []byte) uint64 {
	return uint64(uintptr(unsafe.Pointer(&b[0])))<<32 + uint64(len(b))
}

// Fix for lint rule `unusedfunc`
var _ = begin
var _ = db
var _ = dbstat
var _ = set
var _ = set2
var _ = commit
var _ = abort
var _ = get
var _ = get2
var _ = beginread
var _ = stress
var _ = dbdrop
var _ = del
var _ = cursoropen
var _ = cursorfirst
var _ = cursorput
var _ = cursornext
var _ = cursorcurrent
var _ = cursorclose
var _ = cursordel
var _ = view
var _ = update
var _ = updatefail
var _ = getmissing
var _ = sub
var _ = subabort
var _ = subdel
var _ = clear
var _ = setval
var _ = valptrs2
