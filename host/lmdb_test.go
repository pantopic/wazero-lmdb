package wazero_lmdb

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

//go:embed test\.wasm
var testwasm []byte

func TestModule(t *testing.T) {
	ctx := context.Background()
	r := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().
		WithMemoryLimitPages(256).
		WithMemoryCapacityFromMax(true))
	wasi_snapshot_preview1.MustInstantiate(ctx, r)

	out := &bytes.Buffer{}

	path := "/tmp/pantopic/module-lmdb"
	os.RemoveAll(path)
	module := New(
		WithCtxKeyMeta(`test_meta_key`),
		WithCtxKeyPath(`test_path_key`),
		WithCtxKeyMaxDBs(`test_max_dbs`),
		WithCtxKeyMapSize(`test_map_size`),
	)
	module.Register(ctx, r)

	compiled, err := r.CompileModule(ctx, testwasm)
	if err != nil {
		panic(err)
	}
	cfg := wazero.NewModuleConfig().WithStdout(out)
	mod, err := r.InstantiateModule(ctx, compiled, cfg)
	if err != nil {
		t.Errorf(`%v`, err)
		return
	}

	ctx, err = module.InitContext(ctx, mod)
	if err != nil {
		t.Fatalf(`%v`, err)
	}
	meta := get[*meta](ctx, module.ctxKeyMeta)
	if readUint32(mod, meta.ptrKeyMax) != 511 {
		t.Errorf("incorrect maximum key length: %#v", meta)
	}

	tenantID := 1
	ctx = context.WithValue(ctx, module.ctxKeyPath, fmt.Sprintf(`%s/local/%016x`, path, tenantID))
	ctx = context.WithValue(ctx, module.ctxKeyMaxDBs, 256)
	ctx = context.WithValue(ctx, module.ctxKeyMapSize, int64(16<<20))

	call := func(cmd string, params ...uint64) {
		if _, err := mod.ExportedFunction(cmd).Call(ctx, params...); err != nil {
			t.Fatalf("%v\n%s", err, out.String())
		}
	}
	stat := func(n uint64) {
		stack, err := mod.ExportedFunction("stat").Call(ctx)
		if err != nil {
			t.Fatalf(`%v`, err)
		}
		buf, _ := mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
		if statFromBytes(buf).Entries != n {
			t.Fatalf("Wrong number of entries: %v", string(buf))
		}
	}
	dbstat := func(n uint64) {
		stack, err := mod.ExportedFunction("dbstat").Call(ctx)
		if err != nil {
			t.Fatalf(`%v`, err)
		}
		buf, _ := mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
		if statFromBytes(buf).Entries != n {
			t.Fatalf("Wrong number of entries: %+v", statFromBytes(buf))
		}
	}
	t.Run("open", func(t *testing.T) {
		call("open")
		stat(0)
	})

	t.Run("set", func(t *testing.T) {
		t.Run("commit", func(t *testing.T) {
			call("begin")
			call("db")
			dbstat(0)
			call("set")
			call("commit")
			stat(1)
		})
		t.Run("abort", func(t *testing.T) {
			call("begin")
			dbstat(1)
			call("get")
			call("getmissing")
			call("set2")
			call("abort")
			stat(1)
		})
	})
	t.Run("sync", func(t *testing.T) {
		call("sync")
	})
	t.Run("get", func(t *testing.T) {
		call("begin")
		dbstat(1)
		call("set2")
		call("get2")
		call("commit")
		stat(1)
		call("beginread")
		dbstat(2)
		call("get2")
		call("abort")
	})
	t.Run("del", func(t *testing.T) {
		call("begin")
		call("del")
		dbstat(1)
		call("commit")
		call("beginread")
		dbstat(1)
		call("commit")
	})
	t.Run("update", func(t *testing.T) {
		call("update")
		call("updatefail")
	})
	t.Run("view", func(t *testing.T) {
		call("view")
	})
	t.Run("sub", func(t *testing.T) {
		call("beginread")
		dbstat(1)
		call("commit")
		call("subabort")
		call("beginread")
		dbstat(1)
		call("commit")
		call("sub")
		call("beginread")
		dbstat(2)
		call("commit")
		call("subdel")
		call("beginread")
		dbstat(1)
		call("commit")
	})
	t.Run("cursor", func(t *testing.T) {
		call("begin")
		call("cursoropen")
		call("cursorfirst")
		call("cursorput")
		call("cursorcurrent")
		call("cursorclose")
		call("commit")
		call("beginread")
		dbstat(2)
		call("commit")
		call("begin")
		call("cursoropen")
		call("cursorfirst")
		call("cursornext")
		call("cursordel")
		call("commit")
		call("beginread")
		dbstat(1)
		call("commit")
	})
	t.Run("env", func(t *testing.T) {
		call("close")
		call("open")
		call("open")
	})
	call("open")
	module.Reset(ctx)
	module.TenantSync(ctx)
	module.TenantClose(ctx)
	module.TenantDelete(ctx)
	module.TenantSync(ctx)
	module.TenantClose(ctx)
	module.TenantDelete(ctx)
	var n uint64 = 10_000
	t.Run("stress", func(t *testing.T) {
		call("open")
		start := time.Now()
		call("stress", n)
		t.Logf(`Stress: %v per Put`, time.Since(start)/time.Duration(n))
		call("begin")
		dbstat(n)
		call("commit")
	})
	t.Run("dbdrop", func(t *testing.T) {
		stat(1)
		call("begin")
		call("dbdrop")
		call("commit")
		call("begin")
		call("db")
		dbstat(0)
		call("set")
		call("commit")
		call("beginread")
		dbstat(1)
	})
	t.Run("close", func(t *testing.T) {
		call("close")
		call("close")
	})
	t.Run("delete", func(t *testing.T) {
		call("open")
		call("delete")
		call("delete")
		call("open")
	})
	module.Stop()
}
