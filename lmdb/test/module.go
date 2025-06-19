package main

import (
	"unsafe"

	"github.com/pantopic/plugin-lmdb/lmdb"
)

func main() {}

//export test
func test() uint64 {
	env := lmdb.Open("test")
	data := env.Stat()
	return sliceToPtr(data)
}

var _ = test

func sliceToPtr(b []byte) uint64 {
	return uint64(uintptr(unsafe.Pointer(&b[0])))<<32 + uint64(len(b))
}
