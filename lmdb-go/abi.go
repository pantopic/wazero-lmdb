package lmdb

import (
	"unsafe"
)

var (
	keyCap  uint32 = 511
	valCap  uint32 = 1.5 * 1024 * 1024
	keyLen  uint32
	valLen  uint32
	txnID   uint32
	expDbi  DBI
	curID   uint32
	expFlg  uint32
	errCode uint32
	key     = make([]byte, int(keyCap))
	val     = make([]byte, int(valCap))
	meta    = make([]uint32, 11)
)

//export __lmdb
func __lmdb() (res uint32) {
	meta[0] = uint32(uintptr(unsafe.Pointer(&keyCap)))
	meta[1] = uint32(uintptr(unsafe.Pointer(&keyLen)))
	meta[2] = uint32(uintptr(unsafe.Pointer(&key[0])))
	meta[3] = uint32(uintptr(unsafe.Pointer(&valCap)))
	meta[4] = uint32(uintptr(unsafe.Pointer(&valLen)))
	meta[5] = uint32(uintptr(unsafe.Pointer(&val[0])))
	meta[6] = uint32(uintptr(unsafe.Pointer(&txnID)))
	meta[7] = uint32(uintptr(unsafe.Pointer(&expDbi)))
	meta[8] = uint32(uintptr(unsafe.Pointer(&curID)))
	meta[9] = uint32(uintptr(unsafe.Pointer(&expFlg)))
	meta[10] = uint32(uintptr(unsafe.Pointer(&errCode)))
	return uint32(uintptr(unsafe.Pointer(&meta[0])))
}

func setKey(k []byte) {
	copy(key[:len(k)], k)
	keyLen = uint32(len(k))
}

func getKey() []byte {
	return key[:keyLen]
}

func setVal(v []byte) {
	copy(val[:len(v)], v)
	valLen = uint32(len(v))
}

func getVal() []byte {
	return val[:valLen]
}

//go:wasm-module lmdb
//export Begin
func lmdbBegin()

//go:wasm-module lmdb
//export DbOpen
func lmdbDbOpen()

//go:wasm-module lmdb
//export DbStat
func lmdbDbStat()

//go:wasm-module lmdb
//export DbDrop
func lmdbDbDrop()

//go:wasm-module lmdb
//export Commit
func lmdbCommit()

//go:wasm-module lmdb
//export Abort
func lmdbAbort()

//go:wasm-module lmdb
//export Put
func lmdbPut()

//go:wasm-module lmdb
//export Get
func lmdbGet()

//go:wasm-module lmdb
//export Del
func lmdbDel()

//go:wasm-module lmdb
//export CursorOpen
func lmdbCursorOpen()

//go:wasm-module lmdb
//export CursorGet
func lmdbCursorGet()

//go:wasm-module lmdb
//export CursorPut
func lmdbCursorPut()

//go:wasm-module lmdb
//export CursorDel
func lmdbCursorDel()

//go:wasm-module lmdb
//export CursorClose
func lmdbCursorClose()

// Fix for lint rule `unusedfunc`
var _ = __lmdb
