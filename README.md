# Wazero LMDB

A [wazero](https://pkg.go.dev/github.com/tetratelabs/wazero) host module, ABI and guest SDK providing [LMDB](https://pkg.go.dev/github.com/PowerDNS/lmdb-go/lmdb) for WASI modules.

## Host Module

[![Go Reference](https://godoc.org/github.com/pantopic/wazero-lmdb/host?status.svg)](https://godoc.org/github.com/pantopic/wazero-lmdb/host)
[![Go Report Card](https://goreportcard.com/badge/github.com/pantopic/wazero-lmdb/host)](https://goreportcard.com/report/github.com/pantopic/wazero-lmdb/host)
[![Go Coverage](https://github.com/pantopic/wazero-lmdb/wiki/host/coverage.svg)](https://raw.githack.com/wiki/pantopic/wazero-lmdb/host/coverage.html)

First register the host module with the runtime

```go
import (
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"

	"github.com/pantopic/wazero-lmdb/host"
)

func main() {
	ctx := context.Background()
	r := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig())
	wasi_snapshot_preview1.MustInstantiate(ctx, r)

	module := wazero_lmdb.New()
	module.Register(ctx, r)

	// ...
}
```

## Guest SDK (Go)

[![Go Reference](https://godoc.org/github.com/pantopic/wazero-lmdb/lmdb-go?status.svg)](https://godoc.org/github.com/pantopic/wazero-lmdb/lmdb-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/pantopic/wazero-lmdb/lmdb-go)](https://goreportcard.com/report/github.com/pantopic/wazero-lmdb/lmdb-go)

Then you can import the guest SDK into your WASI module to create and manage LMDB environments from WASM.

```go
package main

import (
	"unsafe"

	"github.com/pantopic/wazero-lmdb/lmdb-go"
)

func main() {}

//export set
func set() {
	lmdb.Update(func(txn *lmdb.Txn) error {
		dbi, _ := txn.DbCreate("test")
		return txn.Put(dbi, []byte(`hello`), []byte(`world`))
	})
}

//export get
func get() uint64 {
	var val []byte
	lmdb.View(func(txn *lmdb.Txn) (err error) {
		dbi, _ := txn.DbOpen("test")
		val, err = txn.Get(dbi, []byte(`hello`))
		return
	})
	return uint64(uintptr(unsafe.Pointer(&val[0])))<<32 + uint64(len(val))
}
```

The [guest SDK](https://pkg.go.dev/github.com/pantopic/wazero-lmdb/lmdb-go) has no dependencies outside the Go std lib.
The [ABI](lmdb/abi.go) is ~130 lines of code and the [SDK](lmdb/sdk.go) is ~400 lines of code so it should be simple
to port this guest SDK if you want to use the Host Module from WASM modules written in other guest languages
(i.e. Rust). Contributions welcome.

Wazero prides itself on having no dependencies and neither does [lmdb-go](https://pkg.go.dev/github.com/PowerDNS/lmdb-go/lmdb)
(apart from CGO), so your [go.sum](go.sum) should remain tidy.

## Roadmap

This project is in alpha. Breaking API changes should be expected until Beta.

- `v0.0.x` - Alpha
  - [ ] Stabilize API
- `v0.x.x` - Beta
  - [ ] Finalize API
  - [ ] Test in production
- `v1.x.x` - General Availability
  - [ ] Proven long term stability in production
