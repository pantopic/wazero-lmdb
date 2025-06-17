package plugin_kv

import (
	"context"
	"log"
	"os"

	"github.com/PowerDNS/lmdb-go/lmdb"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

var (
	ctxKeyMeta = `plugin_kv_meta`
)

type meta struct {
	ptrEnv     uint32
	ptrKey     uint32
	ptrVal     uint32
	ptrDbi     uint32
	maxKeySize uint32
	maxValSize uint32
	txn        map[*lmdb.Env][]*lmdb.Txn
}

type plugin struct {
	env map[uint64]map[string]*lmdb.Env
}

func (p *plugin) Register(ctx context.Context, runtime wazero.Runtime) {
	builder := runtime.NewHostModuleBuilder("kv")
	defer builder.Instantiate(ctx)
	var err error
	var (
		path   = "/tmp/test.mdb"
		optEnv = lmdb.NoMemInit | lmdb.NoReadahead | lmdb.NoSync | lmdb.NoMetaSync
		optDB  = 0

		env *lmdb.Env
		dbi lmdb.DBI
	)
	if err = os.MkdirAll(path, 0777); err != nil {
		panic(err)
	}
	if env, err = lmdb.NewEnv(); err != nil {
		panic(err)
	}
	env.SetMaxDBs(65536)
	env.SetMapSize(int64(1 << 37))
	err = env.Open(path, uint(optEnv), 0777)
	if err != nil {
		panic(err)
	}
	if err = env.Update(func(txn *lmdb.Txn) (err error) {
		if dbi, err = txn.OpenDBI("test", uint(optDB|lmdb.Create)); err != nil {
			panic(err)
		}
		return
	}); err != nil {
		panic(err)
	}
	var meta meta

	for name, fn := range map[string]any{
		"Open":   func(ctx context.Context, name string) (envID uint64) { return 0 },
		"Stat":   func(ctx context.Context, env *lmdb.Env, val []byte) (len uint32) { return 0 },
		"Create": func(b []byte) []byte { return b },
		"Drop":   func(b []byte) []byte { return b },
		"Put": func(ctx context.Context, env *lmdb.Env, dbi lmdb.DBI, key, val []byte) {
			err := txn(ctx, env).Put(dbi, key, val, 0)
			if err != nil {
				panic(err)
			}
		},
		"Get": func(ctx context.Context, env *lmdb.Env, dbi lmdb.DBI, key, val []byte) uint32 {
			v, err := txn(ctx, env).Get(dbi, key)
			if err != nil {
				panic(err)
			}
			copy(v, val[:len(v)])
			return uint32(len(v))
		},
		"Del":     func(b []byte) []byte { return b },
		"Cursor":  func(b []byte) []byte { return b },
		"Next":    func(ctx context.Context, cursorID uint32, key, val []byte) uint32 { return 0 },
		"NextDup": func(b []byte) []byte { return b },
		"Prev":    func(b []byte) []byte { return b },
		"PrevDup": func(b []byte) []byte { return b },
		"Begin": func(ctx context.Context, env *lmdb.Env) {
			txnOpen(ctx, env, true)
		},
		"Commit": func(ctx context.Context, env *lmdb.Env) {
			txnClose(ctx, env, true)
		},
		"Abort": func(ctx context.Context, env *lmdb.Env) {
			txnClose(ctx, env, false)
		},
	} {
		switch fn.(type) {
		case func(context.Context, *lmdb.Env, lmdb.DBI, []byte, []byte) uint32:
			builder = builder.NewFunctionBuilder().WithGoModuleFunction(
				api.GoModuleFunc(func(ctx context.Context, m api.Module, stack []uint64) {
					key := read(m, meta.ptrKey, api.DecodeU32(stack[0]))
					val := read(m, meta.ptrVal, meta.maxValSize)
					res := fn.(func(context.Context, *lmdb.Env, lmdb.DBI, []byte, []byte) uint32)(ctx, env, dbi, key, val)
					stack[0] = api.EncodeU32(res)
				}),
				[]api.ValueType{api.ValueTypeI32},
				[]api.ValueType{api.ValueTypeI32},
			).Export(name)
		case func(context.Context, *lmdb.Env, lmdb.DBI, []byte, []byte):
			builder = builder.NewFunctionBuilder().WithGoModuleFunction(
				api.GoModuleFunc(func(ctx context.Context, m api.Module, stack []uint64) {
					key := read(m, meta.ptrKey, api.DecodeU32(stack[0]))
					val := read(m, meta.ptrVal, api.DecodeU32(stack[1]))
					fn.(func(context.Context, *lmdb.Env, lmdb.DBI, []byte, []byte))(ctx, env, dbi, key, val)
				}),
				[]api.ValueType{},
				[]api.ValueType{api.ValueTypeI32},
			).Export(name)
		case func(context.Context, *lmdb.Env):
			builder = builder.NewFunctionBuilder().WithGoModuleFunction(
				api.GoModuleFunc(func(ctx context.Context, m api.Module, stack []uint64) {
					fn.(func(ctx context.Context, env *lmdb.Env))(ctx, env)
				}), nil, nil).Export(name)
		}
	}
}

func (p *plugin) InitContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKeyMeta, &meta{})
}

func read(m api.Module, ptr, size uint32) (buf []byte) {
	buf, ok := m.Memory().Read(ptr, size)
	if !ok {
		log.Panicf("Memory.Read(%d, %d) out of range", ptr, size)
	}
	return
}

func txn(ctx context.Context, env *lmdb.Env) (txn *lmdb.Txn) {
	v := ctx.Value(ctxKeyMeta)
	if v == nil {
		panic("Context not initialized")
	}
	m := v.(*meta)
	txns, ok := m.txn[env]
	if !ok {
		txns = []*lmdb.Txn{beginTxn(env, nil, false)}
		m.txn[env] = txns
	}
	return txns[len(txns)-1]
}

func txnOpen(ctx context.Context, env *lmdb.Env, readonly bool) (txn *lmdb.Txn) {
	v := ctx.Value(ctxKeyMeta)
	if v == nil {
		panic("Context not initialized")
	}
	m := v.(*meta)
	txns, ok := m.txn[env]
	if !ok {
		txns = []*lmdb.Txn{}
	}
	var parent *lmdb.Txn
	if len(txns) > 0 {
		parent = txns[len(txns)-1]
	}
	txns = append(txns, beginTxn(env, parent, readonly))
	m.txn[env] = txns
	return txns[len(txns)-1]
}

func txnClose(ctx context.Context, env *lmdb.Env, success bool) {
	v := ctx.Value(ctxKeyMeta)
	if v == nil {
		panic("Context not initialized")
	}
	m := v.(*meta)
	txns, ok := m.txn[env]
	if !ok {
		return
	}
	if success {
		txns[len(txns)-1].Commit()
	} else {
		txns[len(txns)-1].Abort()
	}
	if len(m.txn[env]) == 1 {
		delete(m.txn, env)
	} else {
		m.txn[env] = m.txn[env][:len(m.txn[env])-1]
	}
	// Close open cursors for read txns
	return
}

func beginTxn(env *lmdb.Env, parent *lmdb.Txn, readonly bool) (t *lmdb.Txn) {
	var flags uint
	if readonly {
		flags |= lmdb.Readonly
	}
	t, err := env.BeginTxn(parent, flags)
	if err != nil {
		panic(err)
	}
	t.RawRead = true
	return
}
