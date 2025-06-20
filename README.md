# Pantopic LMDB Plugin

A wazero host module, ABI and guest SDK providing LMDB for WASI modules deployed to Pantopic clusters.

[![Go Reference](https://godoc.org/github.com/pantopic/plugin-lmdb?status.svg)](https://godoc.org/github.com/pantopic/plugin-lmdb)
[![License](https://img.shields.io/badge/License-Apache_2.0-dd6600.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Report Card](https://goreportcard.com/badge/github.com/pantopic/plugin-lmdb?4)](https://goreportcard.com/report/github.com/pantopic/plugin-lmdb)
[![Go Coverage](https://github.com/pantopic/plugin-lmdb/wiki/coverage.svg)](https://raw.githack.com/wiki/pantopic/plugin-lmdb/coverage.html)

The guest SDK has no dependencies and is only ~200 lines of code.  
You can import it into your WASI reactor module to create and manage LMDB environments

```go
package main

import (
	"unsafe"

	"github.com/pantopic/plugin-lmdb/lmdb"
)

func main() {}

//export set
func set() {
	lmdb.Open("test", lmdb.Create).Update(func(txn *lmdb.Txn) error {
		txn.Put(txn.DbOpen("dbname", lmdb.Create), []byte(`hello`), []byte(`world`))
		return nil
	})
}

//export get
func get() uint64 {
	var val []byte
	lmdb.Open("test").View(func(txn *lmdb.Txn) {
		val = txn.Get(txn.DbOpen("dbname"), []byte(`hello`))
	})
	return uint64(uintptr(unsafe.Pointer(&val[0])))<<32 + uint64(len(val))
}
```
