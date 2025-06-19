package plugin_lmdb

import (
	"context"
	_ "embed"
	"log"
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

	plugin := New("/tmp/pantopic/plugin-lmdb/test")
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

	ctx = context.WithValue(ctx, ctxKeyShardID, uint64(1))
	if _, err := mod.ExportedFunction("open").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	stack, err := mod.ExportedFunction("stat").Call(ctx)
	buf, ok := mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
	log.Println(string(buf), ok)
	if _, err := mod.ExportedFunction("begin").Call(ctx); err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := mod.ExportedFunction("db").Call(ctx); err != nil {
		t.Errorf("%v", err)
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
	buf, ok = mod.Memory().Read(uint32(stack[0]>>32), uint32(stack[0]))
	log.Println(string(buf), ok)
}
