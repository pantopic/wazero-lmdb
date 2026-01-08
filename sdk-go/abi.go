package lmdb

import (
	"unsafe"
)

var (
	meta           = make([]uint32, 11)
	keyCap  uint32 = 511
	keyLen  uint32
	key     []byte
	valCap  uint32 = 1.5 * 1024 * 1024
	valLen  uint32
	val     []byte
	txnID   uint32
	expDbi  DBI
	curID   uint32
	expFlg  uint32
	errCode uint32
)

//export __lmdb
func __lmdb() (res uint32) {
	key = make([]byte, int(keyCap))
	val = make([]byte, int(valCap))
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

//go:wasm-module pantopic/wazero-lmdb
//export __lmdb_begin
func lmdbBegin()

//go:wasm-module pantopic/wazero-lmdb
//export __lmdb_db_open
func lmdbDbOpen()

//go:wasm-module pantopic/wazero-lmdb
//export __lmdb_db_stat
func lmdbDbStat()

//go:wasm-module pantopic/wazero-lmdb
//export __lmdb_db_drop
func lmdbDbDrop()

//go:wasm-module pantopic/wazero-lmdb
//export __lmdb_commit
func lmdbCommit()

//go:wasm-module pantopic/wazero-lmdb
//export __lmdb_abort
func lmdbAbort()

//go:wasm-module pantopic/wazero-lmdb
//export __lmdb_put
func lmdbPut()

//go:wasm-module pantopic/wazero-lmdb
//export __lmdb_get
func lmdbGet()

//go:wasm-module pantopic/wazero-lmdb
//export __lmdb_del
func lmdbDel()

//go:wasm-module pantopic/wazero-lmdb
//export __lmdb_cursor_open
func lmdbCursorOpen()

//go:wasm-module pantopic/wazero-lmdb
//export __lmdb_cursor_get
func lmdbCursorGet()

//go:wasm-module pantopic/wazero-lmdb
//export __lmdb_cursor_put
func lmdbCursorPut()

//go:wasm-module pantopic/wazero-lmdb
//export __lmdb_cursor_del
func lmdbCursorDel()

//go:wasm-module pantopic/wazero-lmdb
//export __lmdb_cursor_close
func lmdbCursorClose()

// Fix for lint rule `unusedfunc`
var _ = __lmdb
