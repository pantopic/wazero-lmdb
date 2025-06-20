package wazero_lmdb

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

//go:embed test\.wasm
var binary []byte

func TestModule(t *testing.T) {
	ctx := context.Background()
	r := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().
		WithMemoryLimitPages(256).
		WithMemoryCapacityFromMax(true))
	wasi_snapshot_preview1.MustInstantiate(ctx, r)

	path := "/tmp/pantopic/module-lmdb"
	os.RemoveAll(path)
	module := New(
		WithCtxKeyMeta(`meta`),
		WithCtxKeyTenantID(`tenant_id`),
		WithCtxKeyLocalDir(`local_dir`),
		WithCtxKeyBlockDir(`block_dir`),
	)
	module.Register(ctx, r)

	compiled, err := r.CompileModule(ctx, binary)
	if err != nil {
		panic(err)
	}
	cfg := wazero.NewModuleConfig()
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
	ctx = context.WithValue(ctx, module.ctxKeyTenantID, uint64(tenantID))
	ctx = context.WithValue(ctx, module.ctxKeyLocalDir, fmt.Sprintf(`%s/local/%016x`, path, tenantID))
	ctx = context.WithValue(ctx, module.ctxKeyBlockDir, fmt.Sprintf(`%s/block/%016x`, path, tenantID))
	if _, err := mod.ExportedFunction("open").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	stack, err := mod.ExportedFunction("stat").Call(ctx)
	buf, _ := mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
	if !strings.Contains(string(buf), `"Entries":0`) {
		t.Errorf("Wrong number of entries: %v", string(buf))
		return
	}
	if _, err := mod.ExportedFunction("begin").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("db").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	stack, err = mod.ExportedFunction("dbstat").Call(ctx)
	buf, _ = mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
	if !strings.Contains(string(buf), `"Entries":0`) {
		t.Errorf("Wrong number of entries: %v", string(buf))
		return
	}
	if _, err := mod.ExportedFunction("set").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("commit").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	stack, err = mod.ExportedFunction("stat").Call(ctx)
	buf, _ = mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
	if !strings.Contains(string(buf), `"Entries":1`) {
		t.Errorf("Wrong number of entries: %v", string(buf))
		return
	}
	if _, err := mod.ExportedFunction("begin").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	stack, err = mod.ExportedFunction("dbstat").Call(ctx)
	buf, _ = mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
	if !strings.Contains(string(buf), `"Entries":1`) {
		t.Errorf("Wrong number of entries: %v", string(buf))
		return
	}
	if _, err := mod.ExportedFunction("get").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("set2").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("abort").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	stack, err = mod.ExportedFunction("stat").Call(ctx)
	buf, _ = mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
	if !strings.Contains(string(buf), `"Entries":1`) {
		t.Errorf("Wrong number of entries: %v", string(buf))
		return
	}
	if _, err := mod.ExportedFunction("begin").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	stack, err = mod.ExportedFunction("dbstat").Call(ctx)
	buf, _ = mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
	if !strings.Contains(string(buf), `"Entries":1`) {
		t.Errorf("Wrong number of entries: %v", string(buf))
		return
	}
	if _, err := mod.ExportedFunction("set2").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("get2").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("commit").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	stack, err = mod.ExportedFunction("stat").Call(ctx)
	buf, _ = mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
	if !strings.Contains(string(buf), `"Entries":1`) {
		t.Errorf("Wrong number of entries: %s", string(buf))
	}
	if _, err := mod.ExportedFunction("beginread").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	stack, err = mod.ExportedFunction("dbstat").Call(ctx)
	buf, _ = mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
	if !strings.Contains(string(buf), `"Entries":2`) {
		t.Errorf("Wrong number of entries: %v", string(buf))
		return
	}
	if _, err := mod.ExportedFunction("get2").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("abort").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("begin").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("del").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	stack, err = mod.ExportedFunction("dbstat").Call(ctx)
	buf, _ = mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
	if !strings.Contains(string(buf), `"Entries":1`) {
		t.Errorf("Wrong number of entries: %v", string(buf))
		return
	}
	if _, err := mod.ExportedFunction("commit").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("beginread").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	stack, err = mod.ExportedFunction("dbstat").Call(ctx)
	buf, _ = mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
	if !strings.Contains(string(buf), `"Entries":1`) {
		t.Errorf("Wrong number of entries: %v", string(buf))
		return
	}
	if _, err := mod.ExportedFunction("commit").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("update").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("updatefail").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("view").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("begin").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("cursoropen").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("cursorfirst").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("cursorput").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("cursorcurrent").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("cursorclose").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("commit").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("beginread").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	stack, err = mod.ExportedFunction("dbstat").Call(ctx)
	buf, _ = mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
	if !strings.Contains(string(buf), `"Entries":2`) {
		t.Errorf("Wrong number of entries: %v", string(buf))
		return
	}
	if _, err := mod.ExportedFunction("commit").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("begin").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("cursoropen").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("cursorfirst").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("cursornext").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("cursordel").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("commit").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("beginread").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	stack, err = mod.ExportedFunction("dbstat").Call(ctx)
	buf, _ = mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
	if !strings.Contains(string(buf), `"Entries":1`) {
		t.Errorf("Wrong number of entries: %v", string(buf))
		return
	}
	if _, err := mod.ExportedFunction("commit").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("close").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("open").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("open").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("begin").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	module.Reset(ctx)
	module.TenantSync(ctx)
	module.TenantClose(ctx)
	module.TenantDelete(ctx)
	module.TenantSync(ctx)
	module.TenantClose(ctx)
	module.TenantDelete(ctx)
	if _, err := mod.ExportedFunction("open").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	var n uint64 = 10_000
	start := time.Now()
	if _, err := mod.ExportedFunction("stress").Call(ctx, n); err != nil {
		t.Errorf("%v", err)
		return
	}
	t.Logf(`Stress: %v per Put`, time.Since(start)/time.Duration(n))
	if _, err := mod.ExportedFunction("begin").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	stack, err = mod.ExportedFunction("dbstat").Call(ctx)
	buf, _ = mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
	if !strings.Contains(string(buf), fmt.Sprintf(`"Entries":%d`, n)) {
		t.Errorf("Wrong number of entries: %v", string(buf))
		return
	}
	if _, err := mod.ExportedFunction("dbdrop").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("commit").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	stack, err = mod.ExportedFunction("stat").Call(ctx)
	buf, _ = mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
	if !strings.Contains(string(buf), `"Entries":0`) {
		t.Errorf("Wrong number of entries: %v", string(buf))
		return
	}
	if _, err := mod.ExportedFunction("begin").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("db").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	stack, err = mod.ExportedFunction("dbstat").Call(ctx)
	buf, _ = mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
	if !strings.Contains(string(buf), `"Entries":0`) {
		t.Errorf("Wrong number of entries: %v", string(buf))
		return
	}
	if _, err := mod.ExportedFunction("set").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("commit").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("beginread").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	stack, err = mod.ExportedFunction("dbstat").Call(ctx)
	buf, _ = mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
	if !strings.Contains(string(buf), `"Entries":1`) {
		t.Errorf("Wrong number of entries: %v", string(buf))
		return
	}
	if _, err := mod.ExportedFunction("close").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("close").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("openblock").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("begin").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("db").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	stack, err = mod.ExportedFunction("dbstat").Call(ctx)
	buf, _ = mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
	if !strings.Contains(string(buf), `"Entries":0`) {
		t.Errorf("Wrong number of entries: %v", string(buf))
		return
	}
	if _, err := mod.ExportedFunction("set").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("commit").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("beginread").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	stack, err = mod.ExportedFunction("dbstat").Call(ctx)
	buf, _ = mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
	if !strings.Contains(string(buf), `"Entries":1`) {
		t.Errorf("Wrong number of entries: %v", string(buf))
		return
	}
	if _, err := mod.ExportedFunction("delete").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("delete").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("openblock").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	module.Stop()
}
