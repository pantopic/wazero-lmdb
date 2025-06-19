package lmdb

import (
	"unsafe"
)

var (
	maxKeyLen uint32 = 511
	maxValLen uint32 = 1.5 * 1024 * 1024
	keyLen    uint32
	valLen    uint32
	envID     uint32
	dbi       uint32
	cur       uint32
	key       = make([]byte, int(maxKeyLen))
	val       = make([]byte, int(maxValLen))
	meta      = make([]uint32, 9)
)

//export lmdb
func Meta() (res uint32) {
	meta[0] = uint32(uintptr(unsafe.Pointer(&maxKeyLen)))
	meta[1] = uint32(uintptr(unsafe.Pointer(&maxValLen)))
	meta[2] = uint32(uintptr(unsafe.Pointer(&keyLen)))
	meta[3] = uint32(uintptr(unsafe.Pointer(&valLen)))
	meta[4] = uint32(uintptr(unsafe.Pointer(&envID)))
	meta[5] = uint32(uintptr(unsafe.Pointer(&dbi)))
	meta[6] = uint32(uintptr(unsafe.Pointer(&cur)))
	meta[7] = uint32(uintptr(unsafe.Pointer(&key[0])))
	meta[8] = uint32(uintptr(unsafe.Pointer(&val[0])))
	return uint32(uintptr(unsafe.Pointer(&meta[0])))
}

func setKey(k []byte) {
	copy(k, key[:len(k)])
	keyLen = uint32(len(k))
}

func getKey() []byte {
	return val[:keyLen]
}

func setVal(v []byte) {
	copy(v, val[:len(v)])
	valLen = uint32(len(v))
}

func getVal() []byte {
	return val[:valLen]
}

//export lmdbDbCreate
func lmdbDbCreate()

//export lmdbDbOpen
func lmdbDbOpen()

//export lmdbDbDrop
func lmdbDbDrop()

//export lmdbBegin
func lmdbBegin()

//export lmdbCommit
func lmdbCommit()

//export lmdbAbort
func lmdbAbort()

//export lmdbPut
func lmdbPut()

//export lmdbGet
func lmdbGet()

//export lmdbDel
func lmdbDel()

//export lmdbDelDup
func lmdbDelDup()

//export lmdbCursorOpen
func lmdbCursorOpen()

//export lmdbCursorSeek
func lmdbCursorSeek()

//export lmdbCursorGet
func lmdbCursorGet()

//export lmdbCursorDel
func lmdbCursorDel()

//export lmdbCursorNext
func lmdbCursorNext()

//export lmdbCursorNextDup
func lmdbCursorNextDup()

//export lmdbCursorNextNoDup
func lmdbCursorNextNoDup()

//export lmdbCursorPrev
func lmdbCursorPrev()

//export lmdbCursorPrevDup
func lmdbCursorPrevDup()

//export lmdbCursorPrevNoDup
func lmdbCursorPrevNoDup()
