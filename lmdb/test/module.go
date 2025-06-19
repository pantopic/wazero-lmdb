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
	env = lmdb.Open("stat")
}

//export stat
func stat() uint64 {
	data := env.Stat()
	return sliceToPtr(data)
}

//export begin
func begin() {
	txn = env.BeginTxn(nil, true)
}

//export db
func db() {
	dbi = txn.DbCreate("test", 0)
}

//export set
func set() {
	txn.Put(dbi, []byte(`a`), []byte(`1`))
}

//export commit
func commit() {
	txn.Commit()
}

func sliceToPtr(b []byte) uint64 {
	return uint64(uintptr(unsafe.Pointer(&b[0])))<<32 + uint64(len(b))
}
