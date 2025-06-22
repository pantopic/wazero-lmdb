# Wazero LMDB

A [wazero](https://github.com/tetratelabs/wazero) host module, ABI and guest SDK providing [LMDB](https://github.com/PowerDNS/lmdb-go/lmdb) for WASI modules.

[![Go Reference](https://godoc.org/github.com/pantopic/wazero-lmdb/host?status.svg)](https://godoc.org/github.com/pantopic/wazero-lmdb/host)
[![License](https://img.shields.io/badge/License-Apache_2.0-dd6600.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Report Card](https://goreportcard.com/badge/github.com/pantopic/wazero-lmdb/host)](https://goreportcard.com/report/github.com/pantopic/wazero-lmdb/host)
[![Go Coverage](https://github.com/pantopic/wazero-lmdb/wiki/coverage.svg)](https://raw.githack.com/wiki/pantopic/wazero-lmdb/coverage.html)

First register the host module with the runtime

```go
import (
	"github.com/pantopic/wazero-lmdb/host"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
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

Then you can import the guest SDK into your WASI module to create and manage LMDB environments from WASM.

```go
package main

import (
	"unsafe"

	"github.com/pantopic/wazero-lmdb/lmdb"
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

The [guest SDK](https://pkg.go.dev/github.com/pantopic/wazero-lmdb/lmdb) has no dependencies outside the Go std lib.
The [ABI](lmdb/abi.go) is ~130 lines of code and the [SDK](lmdb/sdk.go) is ~200 lines of code so it should be simple
to port this guest SDK if you want to use use this Host Module in other guest languages (i.e. Rust).
Contributions welcome.

Wazero prides itself on having no dependencies and neither does [lmdb-go](https://github.com/PowerDNS/lmdb-go/lmdb) so your
[go.sum](go.sum) should remain tidy.

## Roadmap

This project is in alpha. Breaking API changes should be expected until Beta.

- `v0.0.x` - Alpha
  - [ ] Stabilize API
- `v0.x.x` - Beta
  - [ ] Finalize API
  - [ ] Test in production
- `v1.x.x` - General Availability
  - [ ] Proven long term stability in production

## License

Licensed under Apache 2.0
