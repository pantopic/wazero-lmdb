package lmdb

import (
	"unsafe"
)

var (
	maxKey  uint32 = 511
	maxVal  uint32 = 1.5 * 1024 * 1024
	keyLen  uint32
	valLen  uint32
	envID   uint32
	txnID   uint32
	expDbi  DBI
	curID   uint32
	expFlg  uint32
	errCode uint32
	key     = make([]byte, int(maxKey))
	val     = make([]byte, int(maxVal))
	meta    = make([]uint32, 12)
)

//export lmdb
func lmdb() (res uint32) {
	meta[0] = uint32(uintptr(unsafe.Pointer(&maxKey)))
	meta[1] = uint32(uintptr(unsafe.Pointer(&maxVal)))
	meta[2] = uint32(uintptr(unsafe.Pointer(&keyLen)))
	meta[3] = uint32(uintptr(unsafe.Pointer(&valLen)))
	meta[4] = uint32(uintptr(unsafe.Pointer(&envID)))
	meta[5] = uint32(uintptr(unsafe.Pointer(&txnID)))
	meta[6] = uint32(uintptr(unsafe.Pointer(&expDbi)))
	meta[7] = uint32(uintptr(unsafe.Pointer(&curID)))
	meta[8] = uint32(uintptr(unsafe.Pointer(&expFlg)))
	meta[9] = uint32(uintptr(unsafe.Pointer(&errCode)))
	meta[10] = uint32(uintptr(unsafe.Pointer(&key[0])))
	meta[11] = uint32(uintptr(unsafe.Pointer(&val[0])))
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
//export EnvOpen
func lmdbEnvOpen()

//go:wasm-module lmdb
//export EnvStat
func lmdbEnvStat()

//go:wasm-module lmdb
//export EnvClose
func lmdbEnvClose()

//go:wasm-module lmdb
//export EnvDelete
func lmdbEnvDelete()

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
var _ = lmdb
