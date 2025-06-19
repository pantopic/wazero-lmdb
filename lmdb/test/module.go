package main

import (
	"unsafe"

	"github.com/pantopic/plugin-lmdb/lmdb"
)

func main() {}

//export test
func test() (uint32, uint32) {
	env := lmdb.Open("test")
	data := env.Stat()
	// data := stat.MarshalTo(nil)
	return sliceToPtr(data)
}

var _ = test

func sliceToPtr(b []byte) (uint32, uint32) {
	return uint32(uintptr(unsafe.Pointer(&b[0]))), uint32(len(b))
}
