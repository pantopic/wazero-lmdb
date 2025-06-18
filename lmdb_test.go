package plugin_lmdb

import (
	"context"
	_ "embed"
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

	compiled, err := r.CompileModule(ctx, binary)
	if err != nil {
		panic(err)
	}
	cfg := wazero.NewModuleConfig()
	mod, err := r.InstantiateModule(ctx, compiled, cfg.WithName("test"))
	if err != nil {
		t.Errorf(`%v`, err)
	}

	p := New("/tmp/pantopic/plugin-lmdb/test")
	p.Register(ctx, r)
	ctx2 := p.InitContext(ctx, mod)
	meta := get[*meta](ctx2, ctxKeyMeta)
	if readUint32(mod, meta.keyMax) != 511 {
		t.Errorf("incorrect maximum key length: %#v", meta)
	}
}
