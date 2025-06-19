package main

import (
	"unsafe"

	"github.com/pantopic/plugin-lmdb/lmdb"
)

func main() {}

var env *lmdb.Env
var txn *lmdb.Txn
var dbi uint32

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

//export abort
func abort() {
	txn.Abort()
}

//export close
func close() {
	env.Close()
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
