package main

import (
	"unsafe"
)

var (
	maxKeySize uint32 = 511
	maxValSize uint32 = 1.5 * 1024 * 1024
	key               = make([]byte, int(maxKeySize))
	val               = make([]byte, int(maxValSize))
	env        uint32
	dbi        uint32

	meta []uint32
)

func main() {
	meta = []uint32{
		uint32(uintptr(unsafe.Pointer(&maxKeySize))),
		uint32(uintptr(unsafe.Pointer(&maxValSize))),
		uint32(uintptr(unsafe.Pointer(&key[0]))),
		uint32(uintptr(unsafe.Pointer(&val[0]))),
		uint32(uintptr(unsafe.Pointer(&env))),
		uint32(uintptr(unsafe.Pointer(&dbi))),
	}
}

// kv returns a pointer to the location of the meta pointer array
//
//export kv
func kv() (res uint32) {
	return uint32(uintptr(unsafe.Pointer(&meta[0])))
}

// Fix for `unusedfunc` lint warnings
var _ = kv()
