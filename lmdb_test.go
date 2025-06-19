package plugin_lmdb

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

//go:embed test\.wasm
var binary []byte

func TestPlugin(t *testing.T) {
	ctx := context.Background()
	runtimeConfig := wazero.NewRuntimeConfig().
		WithCoreFeatures(api.CoreFeaturesV2)
	runtimeConfig = runtimeConfig.WithMemoryLimitPages(256).WithMemoryCapacityFromMax(true)
	r := wazero.NewRuntimeWithConfig(ctx, runtimeConfig)
	wasi_snapshot_preview1.MustInstantiate(ctx, r)

	path := "/tmp/pantopic/plugin-lmdb"
	os.RemoveAll(path)
	plugin := New()
	plugin.Register(ctx, r)

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

	ctx = plugin.InitContext(ctx, mod)
	meta := get[*meta](ctx, ctxKeyMeta)
	if readUint32(mod, meta.keyMax) != 511 {
		t.Errorf("incorrect maximum key length: %#v", meta)
		return
	}

	shardID := 1
	ctx = context.WithValue(ctx, ctxKeyShardID, uint64(shardID))
	ctx = context.WithValue(ctx, ctxKeyDataDir, fmt.Sprintf(`%s/%016x`, path, shardID))
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
	if _, err := mod.ExportedFunction("get").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("get2").Call(ctx); err != nil {
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
	plugin.Reset(ctx)
	plugin.ShardSync(1)
	plugin.ShardClose(1)
	plugin.ShardDelete(ctx)
	plugin.ShardSync(1)
	plugin.ShardClose(1)
	plugin.ShardDelete(ctx)
	if _, err := mod.ExportedFunction("open").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	plugin.Stop()
}
