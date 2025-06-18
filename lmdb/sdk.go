package lmdb

import (
	"unsafe"
)

var (
	maxKeyLen uint32 = 511
	maxValLen uint32 = 1.5 * 1024 * 1024
	keyLen    uint32
	valLen    uint32
	env       uint32
	dbi       uint32
	cur       uint32
	key       = make([]byte, int(maxKeyLen))
	val       = make([]byte, int(maxValLen))
	meta      = make([]uint32, 9)
)

// lmdb returns a pointer to the location of the meta pointer array
//
//export lmdb
func Meta() (res uint32) {
	meta[0] = uint32(uintptr(unsafe.Pointer(&maxKeyLen)))
	meta[1] = uint32(uintptr(unsafe.Pointer(&maxValLen)))
	meta[2] = uint32(uintptr(unsafe.Pointer(&keyLen)))
	meta[3] = uint32(uintptr(unsafe.Pointer(&valLen)))
	meta[4] = uint32(uintptr(unsafe.Pointer(&env)))
	meta[5] = uint32(uintptr(unsafe.Pointer(&dbi)))
	meta[6] = uint32(uintptr(unsafe.Pointer(&cur)))
	meta[7] = uint32(uintptr(unsafe.Pointer(&key[0])))
	meta[8] = uint32(uintptr(unsafe.Pointer(&val[0])))
	return uint32(uintptr(unsafe.Pointer(&meta[0])))
}
