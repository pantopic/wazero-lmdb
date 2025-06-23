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
	)
	module.Register(ctx, r)

	compiled, err := r.CompileModule(ctx, testwasm)
	if err != nil {
		panic(err)
	}
	cfg := wazero.NewModuleConfig().WithStdout(out)
	mod, err := r.InstantiateModule(ctx, compiled, cfg.WithName("test"))
	if err != nil {
		t.Errorf(`%v`, err)
		return
	}

	ctx = module.InitContext(ctx, mod)
	meta := get[*meta](ctx, module.ctxKeyMeta)
	if readUint32(mod, meta.ptrKeyMax) != 511 {
		t.Errorf("incorrect maximum key length: %#v", meta)
		return
	}

	tenantID := 1
	ctx = context.WithValue(ctx, module.ctxKeyPath, fmt.Sprintf(`%s/local/%016x`, path, tenantID))

	call := func(cmd string, params ...uint64) bool {
		if _, err := mod.ExportedFunction(cmd).Call(ctx, params...); err != nil {
			t.Errorf("%v\n%s", err, out.String())
			panic(1)
			return false
		}
		return true
	}
	stat := func(n uint64) bool {
		stack, err := mod.ExportedFunction("stat").Call(ctx)
		if err != nil {
			panic(err)
		}
		buf, _ := mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
		if statFromBytes(buf).Entries != n {
			t.Errorf("Wrong number of entries: %v", string(buf))
			panic(1)
			return false
		}
		return true
	}
	dbstat := func(n uint64) bool {
		stack, err := mod.ExportedFunction("dbstat").Call(ctx)
		if err != nil {
			panic(err)
		}
		buf, _ := mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
		if statFromBytes(buf).Entries != n {
			t.Errorf("Wrong number of entries: %+v", statFromBytes(buf))
			panic(1)
			return false
		}
		return true
	}
	t.Run("open", func(t *testing.T) {
		if !call("open") {
			return
		}
		if !stat(0) {
			return
		}
	})

	t.Run("set", func(t *testing.T) {
		t.Run("commit", func(t *testing.T) {
			if !call("begin") {
				return
			}
			if !call("db") {
				return
			}
			if !dbstat(0) {
				return
			}
			if !call("set") {
				return
			}
			if !call("commit") {
				return
			}
			if !stat(1) {
				return
			}
		})
		t.Run("abort", func(t *testing.T) {
			if !call("begin") {
				return
			}
			if !dbstat(1) {
				return
			}
			if !call("get") {
				return
			}
			if !call("getmissing") {
				return
			}
			if !call("set2") {
				return
			}
			if !call("abort") {
				return
			}
			if !stat(1) {
				return
			}
		})
	})
	t.Run("sync", func(t *testing.T) {
		if !call("sync") {
			return
		}
	})
	t.Run("get", func(t *testing.T) {
		if !call("begin") {
			return
		}
		if !dbstat(1) {
			return
		}
		if !call("set2") {
			return
		}
		if !call("get2") {
			return
		}
		if !call("commit") {
			return
		}
		if !stat(1) {
			return
		}
		if !call("beginread") {
			return
		}
		if !dbstat(2) {
			return
		}
		if !call("get2") {
			return
		}
		if !call("abort") {
			return
		}
	})
	t.Run("del", func(t *testing.T) {
		if !call("begin") {
			return
		}
		if !call("del") {
			return
		}
		if !dbstat(1) {
			return
		}
		if !call("commit") {
			return
		}
		if !call("beginread") {
			return
		}
		if !dbstat(1) {
			return
		}
		if !call("commit") {
			return
		}
	})
	t.Run("update", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			if !call("update") {
				return
			}
		})
		t.Run("failure", func(t *testing.T) {
			if !call("updatefail") {
				return
			}
		})
	})
	t.Run("view", func(t *testing.T) {
		if !call("view") {
			return
		}
	})
	t.Run("cursor", func(t *testing.T) {
		if !call("begin") {
			return
		}
		if !call("cursoropen") {
			return
		}
		if !call("cursorfirst") {
			return
		}
		if !call("cursorput") {
			return
		}
		if !call("cursorcurrent") {
			return
		}
		if !call("cursorclose") {
			return
		}
		if !call("commit") {
			return
		}
		if !call("beginread") {
			return
		}
		if !dbstat(2) {
			return
		}
		if !call("commit") {
			return
		}
		if !call("begin") {
			return
		}
		if !call("cursoropen") {
			return
		}
		if !call("cursorfirst") {
			return
		}
		if !call("cursornext") {
			return
		}
		if !call("cursordel") {
			return
		}
		if !call("commit") {
			return
		}
		if !call("beginread") {
			return
		}
		if !dbstat(1) {
			return
		}
		if !call("commit") {
			return
		}
	})
	t.Run("env", func(t *testing.T) {
		if !call("close") {
			return
		}
		if !call("open") {
			return
		}
		if !call("open") {
			return
		}
	})
	if !call("open") {
		return
	}
	module.Reset(ctx)
	module.TenantSync(ctx)
	module.TenantClose(ctx)
	module.TenantDelete(ctx)
	module.TenantSync(ctx)
	module.TenantClose(ctx)
	module.TenantDelete(ctx)

	var n uint64 = 10_000
	t.Run("stress", func(t *testing.T) {
		if !call("open") {
			return
		}
		start := time.Now()
		if !call("stress", n) {
			return
		}
		t.Logf(`Stress: %v per Put`, time.Since(start)/time.Duration(n))
		if !call("begin") {
			return
		}
		if !dbstat(n) {
			return
		}
		if !call("commit") {
			return
		}
	})
	t.Run("dbdrop", func(t *testing.T) {
		if !stat(1) {
			return
		}
		if !call("begin") {
			return
		}
		if !call("dbdrop") {
			return
		}
		if !call("commit") {
			return
		}
		if !call("begin") {
			return
		}
		if !call("db") {
			return
		}
		if !dbstat(0) {
			return
		}
		if !call("set") {
			return
		}
		if !call("commit") {
			return
		}
		if !call("beginread") {
			return
		}
		if !dbstat(1) {
			return
		}
	})
	t.Run("close", func(t *testing.T) {
		if !call("close") {
			return
		}
		if !call("close") {
			return
		}
	})
	t.Run("delete", func(t *testing.T) {
		if !call("delete") {
			return
		}
		if !call("delete") {
			return
		}
		if !call("open") {
			return
		}
	})
	module.Stop()
}
