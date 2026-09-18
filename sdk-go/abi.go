package mdb

import (
	"unsafe"
)

var (
	meta = make([]uint32, 11)

	keyCap uint32 = 511
	keyLen uint32
	key    = make([]byte, int(keyCap))

	valCap uint32 = 1.5 * 1024 * 1024
	valLen uint32
	val    = make([]byte, int(valCap))

	txnID   uint32
	expDbi  DBI
	curID   uint32
	expFlg  uint32
	errCode uint32
)

//export __mdb
func __mdb() (res uint32) {
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

//go:wasm-module pantopic/ext-mdb
//export __mdb_begin
func mdbBegin()

//go:wasm-module pantopic/ext-mdb
//export __mdb_db_open
func mdbDbOpen()

//go:wasm-module pantopic/ext-mdb
//export __mdb_db_stat
func mdbDbStat()

//go:wasm-module pantopic/ext-mdb
//export __mdb_db_drop
func mdbDbDrop()

//go:wasm-module pantopic/ext-mdb
//export __mdb_commit
func mdbCommit()

//go:wasm-module pantopic/ext-mdb
//export __mdb_abort
func mdbAbort()

//go:wasm-module pantopic/ext-mdb
//export __mdb_put
func mdbPut()

//go:wasm-module pantopic/ext-mdb
//export __mdb_get
func mdbGet()

//go:wasm-module pantopic/ext-mdb
//export __mdb_del
func mdbDel()

//go:wasm-module pantopic/ext-mdb
//export __mdb_cursor_open
func mdbCursorOpen()

//go:wasm-module pantopic/ext-mdb
//export __mdb_cursor_get
func mdbCursorGet()

//go:wasm-module pantopic/ext-mdb
//export __mdb_cursor_put
func mdbCursorPut()

//go:wasm-module pantopic/ext-mdb
//export __mdb_cursor_del
func mdbCursorDel()

//go:wasm-module pantopic/ext-mdb
//export __mdb_cursor_close
func mdbCursorClose()

// Fix for lint rule `unusedfunc`
var _ = __mdb
