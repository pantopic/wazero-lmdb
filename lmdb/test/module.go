package main

import (
	"strconv"
	"unsafe"

	"github.com/pantopic/plugin-lmdb/lmdb"
)

func main() {}

var env *lmdb.Env
var txn *lmdb.Txn
var dbi uint32
var cur *lmdb.Cursor

//export open
func open() {
	env = lmdb.Open("test", lmdb.Create)
}

//export stat
func stat() uint64 {
	data := env.Stat()
	return sliceToPtr(data)
}

//export begin
func begin() {
	txn = env.BeginTxn(nil, 0)
}

//export beginread
func beginread() {
	txn = env.BeginTxn(nil, lmdb.Readonly)
}

//export db
func db() {
	dbi = txn.DbOpen("test", lmdb.Create)
}

//export dbstat
func dbstat() uint64 {
	data := txn.Stat(dbi)
	return sliceToPtr(data)
}

//export dbdrop
func dbdrop() {
	txn.DbDrop(dbi)
}

//export set
func set() {
	txn.Put(dbi, []byte(`a`), []byte(`1`))
}

//export get
func get() {
	v := txn.Get(dbi, []byte(`a`))
	if string(v) != `1` {
		panic(`wrong value`)
	}
}

//export del
func del() {
	txn.Del(dbi, []byte(`a`), nil)
}

//export commit
func commit() {
	txn.Commit()
}

//export set2
func set2() {
	txn.Put(dbi, []byte(`b`), []byte(`2`))
}

//export get2
func get2() {
	v := txn.Get(dbi, []byte(`b`))
	if string(v) != `2` {
		panic(`wrong value`)
	}
}

//export stress
func stress(limit uint32) {
	txn = env.BeginTxn(nil, 0)
	dbi = txn.DbOpen("test", lmdb.Create)
	n := int64(limit)
	for i := range n {
		txn.Put(dbi, []byte(strconv.FormatInt(i+1e15, 16)), []byte(strconv.FormatInt(n-i+1e15, 16)))
	}
	txn.Commit()
}

//export abort
func abort() {
	txn.Abort()
}

//export close
func close() {
	env.Close()
}

//export cursoropen
func cursoropen() {
	dbi = txn.DbOpen("test", lmdb.Create)
	cur = txn.CursorOpen(dbi)
}

//export cursorfirst
func cursorfirst() {
	k, v := cur.Get(nil, nil, lmdb.First)
	if string(k) != `b` {
		panic(`wrong key: ` + string(k))
	}
	if string(v) != `2` {
		panic(`wrong value: ` + string(v))
	}
}

//export cursorput
func cursorput() {
	cur.Put([]byte(`c`), []byte(`3`), 0)
}

//export cursorcurrent
func cursorcurrent() {
	k, v := cur.Get(nil, nil, lmdb.GetCurrent)
	if string(k) != `c` {
		panic(`wrong key: ` + string(k))
	}
	if string(v) != `3` {
		panic(`wrong value: ` + string(v))
	}
}

//export cursornext
func cursornext() {
	k, v := cur.Get(nil, nil, lmdb.Next)
	if string(k) != `c` {
		panic(`wrong key: ` + string(k))
	}
	if string(v) != `3` {
		panic(`wrong value: ` + string(v))
	}
}

//export cursorclose
func cursorclose() {
	cur.Close()
}

func sliceToPtr(b []byte) uint64 {
	return uint64(uintptr(unsafe.Pointer(&b[0])))<<32 + uint64(len(b))
}

// Fix for lint rule `unusedfunc`
var _ = open
var _ = stat
var _ = begin
var _ = db
var _ = dbstat
var _ = set
var _ = set2
var _ = commit
var _ = abort
var _ = close
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
