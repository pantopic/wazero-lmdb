package wazero_lmdb

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

//go:embed test\.wasm
var testwasm []byte

func TestModule(t *testing.T) {
	var (
		optEnv uint = lmdb.NoMemInit | lmdb.NoReadahead | lmdb.NoSync | lmdb.NoMetaSync | lmdb.NoLock | lmdb.NoSubdir | lmdb.Create
		path        = "/tmp/pantopic/module-lmdb"
		ctx         = context.Background()
		out         = &bytes.Buffer{}
	)
	r := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().
		WithMemoryLimitPages(256).
		WithMemoryCapacityFromMax(true))
	wasi_snapshot_preview1.MustInstantiate(ctx, r)

	os.RemoveAll(path)
	hostModule := New(
		WithCtxKeyMeta(`test_meta_key`),
		WithCtxKeyEnv(`test_path_env`),
	)
	hostModule.Register(ctx, r)

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

	ctx, err = hostModule.InitContext(ctx, mod)
	if err != nil {
		t.Fatalf(`%v`, err)
	}
	meta := get[*meta](ctx, hostModule.ctxKeyMeta)
	if readUint32(mod, meta.ptrKeyMax) != 511 {
		t.Errorf("incorrect maximum key length: %#v", meta)
	}

	env, err := lmdb.NewEnv()
	if err != nil {
		t.Fatalf(`%v`, err)
	}
	if err = env.SetMapSize(int64(16 << 20)); err != nil {
		t.Fatalf(`%v`, err)
	}
	if err = env.SetMaxDBs(256); err != nil {
		t.Fatalf(`%v`, err)
	}
	path = fmt.Sprintf(`%s/local/%016x`, path, 123)
	if err = os.MkdirAll(path, 0755); err != nil {
		t.Fatalf(`%v`, err)
	}
	if err = env.Open(fmt.Sprintf(`%s/%s.mdb`, path, `data`), optEnv|lmdb.Create, 0700); err != nil {
		t.Fatalf(`%v`, err)
	}
	ctx = context.WithValue(ctx, hostModule.ctxKeyEnv, env)

	call := func(cmd string, params ...uint64) {
		if _, err := mod.ExportedFunction(cmd).Call(ctx, params...); err != nil {
			t.Fatalf("%v\n%s", err, out.String())
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
	t.Run("set", func(t *testing.T) {
		t.Run("commit", func(t *testing.T) {
			call("begin")
			call("db")
			dbstat(0)
			call("set")
			call("commit")
		})
		t.Run("abort", func(t *testing.T) {
			call("begin")
			dbstat(1)
			call("get")
			call("getmissing")
			call("set2")
			call("abort")
		})
	})
	t.Run("get", func(t *testing.T) {
		call("begin")
		dbstat(1)
		call("set2")
		call("get2")
		call("commit")
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
	hostModule.Reset(ctx)
	call("clear")
	var n uint64 = 10_000
	t.Run("stress", func(t *testing.T) {
		start := time.Now()
		call("stress", n)
		t.Logf(`Stress: %v per Put`, time.Since(start)/time.Duration(n))
		call("begin")
		dbstat(n)
		call("commit")
	})
	t.Run("dbdrop", func(t *testing.T) {
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
	hostModule.Stop()
}
