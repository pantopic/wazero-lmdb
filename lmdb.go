package plugin_lmdb

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"sync"

	"github.com/PowerDNS/lmdb-go/lmdb"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

const (
	// Flag for EnvOpen indicating selection of block storage directory rather than local storage.
	Block uint = 1 << 31
)

var (
	DefaultCtxKeyMeta     = `plugin_lmdb_meta`
	DefaultCtxKeyTenantID = `tenant_id`
	DefaultCtxKeyLocalDir = `tenant_local_dir`
	DefaultCtxKeyBlockDir = `tenant_block_dir`

	optEnv    uint = lmdb.NoMemInit | lmdb.NoReadahead | lmdb.NoSync | lmdb.NoMetaSync | lmdb.NoLock | lmdb.NoSubdir
	openFlags uint = lmdb.NoReadahead | lmdb.Create
	dbFlags   uint = lmdb.Create | lmdb.DupSort
	txnFlags  uint = lmdb.Readonly
)

type meta struct {
	ptrKeyMax uint32
	ptrValMax uint32
	ptrKeyLen uint32
	ptrValLen uint32
	ptrEnv    uint32
	ptrTxn    uint32
	ptrDbi    uint32
	ptrCur    uint32
	ptrFlg    uint32
	ptrKey    uint32
	ptrVal    uint32
	txn       map[uint32]*lmdb.Txn
	cursor    map[uint32]*lmdb.Cursor
	txnID     uint32
	curID     uint32
}

type shard struct {
	sync.RWMutex

	envID    uint32
	envNames map[string]uint32
	envs     map[uint32]*lmdb.Env
}

func newShard() *shard {
	return &shard{
		envNames: map[string]uint32{},
		envs:     map[uint32]*lmdb.Env{},
	}
}

type plugin struct {
	sync.RWMutex

	ctxKeyMeta     string
	ctxKeyTenantID string
	ctxKeyLocalDir string
	ctxKeyBlockDir string
	shards         map[uint64]*shard
}

func New(opts ...Option) *plugin {
	p := &plugin{
		ctxKeyMeta:     DefaultCtxKeyMeta,
		ctxKeyTenantID: DefaultCtxKeyTenantID,
		ctxKeyLocalDir: DefaultCtxKeyLocalDir,
		ctxKeyBlockDir: DefaultCtxKeyBlockDir,
		shards:         map[uint64]*shard{},
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (p *plugin) InitContext(ctx context.Context, m api.Module) context.Context {
	init := m.ExportedFunction(`lmdb`)
	stack, err := init.Call(ctx)
	if err != nil {
		panic(err)
	}
	meta := &meta{
		txn:    make(map[uint32]*lmdb.Txn),
		cursor: make(map[uint32]*lmdb.Cursor),
	}
	ptr := uint32(stack[0])
	meta.ptrKeyMax, _ = m.Memory().ReadUint32Le(ptr)
	meta.ptrValMax, _ = m.Memory().ReadUint32Le(ptr + 4)
	meta.ptrKeyLen, _ = m.Memory().ReadUint32Le(ptr + 8)
	meta.ptrValLen, _ = m.Memory().ReadUint32Le(ptr + 12)
	meta.ptrEnv, _ = m.Memory().ReadUint32Le(ptr + 16)
	meta.ptrTxn, _ = m.Memory().ReadUint32Le(ptr + 20)
	meta.ptrDbi, _ = m.Memory().ReadUint32Le(ptr + 24)
	meta.ptrCur, _ = m.Memory().ReadUint32Le(ptr + 28)
	meta.ptrFlg, _ = m.Memory().ReadUint32Le(ptr + 32)
	meta.ptrKey, _ = m.Memory().ReadUint32Le(ptr + 36)
	meta.ptrVal, _ = m.Memory().ReadUint32Le(ptr + 40)
	return context.WithValue(ctx, p.ctxKeyMeta, meta)
}

func (p *plugin) Register(ctx context.Context, runtime wazero.Runtime) {
	builder := runtime.NewHostModuleBuilder("lmdb")
	defer func() {
		if _, err := builder.Instantiate(ctx); err != nil {
			panic(err)
		}
	}()
	register := func(name string, fn func(ctx context.Context, m api.Module, stack []uint64)) {
		builder = builder.NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(fn), nil, nil).Export(name)
	}
	for name, fn := range map[string]any{
		"EnvOpen": func(ctx context.Context, name string, flags uint32) uint32 {
			shardID := get[uint64](ctx, p.ctxKeyTenantID)
			p.RLock()
			shard, ok := p.shards[shardID]
			p.RUnlock()
			if !ok {
				p.Lock()
				shard, ok = p.shards[shardID]
				if !ok {
					shard = newShard()
					p.shards[shardID] = shard
				}
				p.Unlock()
			}
			if ok {
				shard.RLock()
				envID, ok := shard.envNames[name]
				shard.RUnlock()
				if ok {
					return envID
				}
			}
			var path string
			if uint(flags)&Block > 0 {
				path = get[string](ctx, p.ctxKeyBlockDir)
			} else {
				path = get[string](ctx, p.ctxKeyLocalDir)
			}
			if err := os.MkdirAll(path, 0755); err != nil {
				panic(err)
			}
			env, err := lmdb.NewEnv()
			if err != nil {
				panic(err)
			}
			env.SetMaxDBs(1 << 16)
			env.SetMapSize(int64(1 << 37))
			path += "/" + name + ".mdb"
			err = env.Open(path, optEnv|(uint(flags)&openFlags), 0700)
			if err != nil {
				panic(err)
			}
			shard.Lock()
			defer shard.Unlock()
			shard.envID++
			shard.envs[shard.envID] = env
			shard.envNames[name] = shard.envID
			return shard.envID
		},
		"EnvStat": func(env *lmdb.Env) *lmdb.Stat {
			stat, err := env.Stat()
			if err != nil {
				panic(err)
			}
			return stat
		},
		"EnvClose": func(ctx context.Context, envID uint32) {
			shardID := get[uint64](ctx, p.ctxKeyTenantID)
			p.Lock()
			defer p.Unlock()
			shard, ok := p.shards[shardID]
			if !ok {
				return
			}
			shard.Lock()
			defer shard.Unlock()
			env, ok := shard.envs[envID]
			if !ok {
				return
			}
			if err := env.Close(); err != nil {
				panic(err.Error())
			}
			delete(shard.envs, envID)
			for name, id := range shard.envNames {
				if id == envID {
					delete(shard.envNames, name)
				}
			}
			if len(shard.envs) == 0 {
				delete(p.shards, shardID)
			}
		},
		"EnvDelete": func(ctx context.Context, envID uint32) {
			shardID := get[uint64](ctx, p.ctxKeyTenantID)
			p.Lock()
			defer p.Unlock()
			shard, ok := p.shards[shardID]
			if !ok {
				return
			}
			shard.Lock()
			defer shard.Unlock()
			env, ok := shard.envs[envID]
			if !ok {
				return
			}
			path, err := env.Path()
			if err != nil {
				panic(err)
			}
			if err := env.Close(); err != nil {
				panic(err)
			}
			if err := os.Remove(path); err != nil {
				panic(err)
			}
			delete(shard.envs, envID)
			for name, id := range shard.envNames {
				if id == envID {
					delete(shard.envNames, name)
				}
			}
		},
		"Begin": func(env *lmdb.Env, parent *lmdb.Txn, flags uint32) *lmdb.Txn {
			return beginTxn(env, parent, flags)
		},
		"Commit": func(txn *lmdb.Txn) {
			txn.Commit()
		},
		"Abort": func(txn *lmdb.Txn) {
			txn.Abort()
		},
		"DbOpen": func(txn *lmdb.Txn, name string, flags uint32) (dbi lmdb.DBI) {
			dbi, err := txn.OpenDBI(name, uint(flags)&dbFlags)
			if err != nil && !lmdb.IsNotFound(err) {
				panic(err)
			}
			return
		},
		"DbStat": func(txn *lmdb.Txn, dbi lmdb.DBI) *lmdb.Stat {
			stat, err := txn.Stat(dbi)
			if err != nil {
				panic(err)
			}
			return stat
		},
		"DbDrop": func(txn *lmdb.Txn, dbi lmdb.DBI) {
			err := txn.Drop(dbi, true)
			if err != nil && !lmdb.IsNotFound(err) {
				panic(err)
			}
		},
		"Put": func(txn *lmdb.Txn, dbi lmdb.DBI, key, val []byte) {
			err := txn.Put(dbi, key, val, 0)
			if err != nil {
				panic(err)
			}
		},
		"Get": func(txn *lmdb.Txn, dbi lmdb.DBI, key, val []byte) []byte {
			v, err := txn.Get(dbi, key)
			if err != nil && !lmdb.IsNotFound(err) {
				panic(err)
			}
			val = val[:len(v)]
			copy(val, v)
			return val
		},
		"Del": func(txn *lmdb.Txn, dbi lmdb.DBI, key, val []byte) {
			err := txn.Del(dbi, key, val)
			if err != nil && !lmdb.IsNotFound(err) {
				panic(err)
			}
		},
		"CursorOpen": func(txn *lmdb.Txn, dbi lmdb.DBI) *lmdb.Cursor {
			cur, err := txn.OpenCursor(dbi)
			if err != nil {
				panic(err)
			}
			return cur
		},
		"CursorGet": func(cur *lmdb.Cursor, key, val []byte, flags uint32) ([]byte, []byte) {
			k, v, err := cur.Get(key, val, uint(flags))
			if err != nil && !lmdb.IsNotFound(err) {
				panic(err)
			}
			key = key[:len(k)]
			copy(key, k)
			val = val[:len(v)]
			copy(val, v)
			return key, val
		},
		"CursorDel": func(cur *lmdb.Cursor, flags uint32) {
			err := cur.Del(uint(flags))
			if err != nil && !lmdb.IsNotFound(err) {
				panic(err)
			}
		},
		"CursorPut": func(cur *lmdb.Cursor, key, val []byte, flags uint32) {
			err := cur.Put(key, val, uint(flags))
			if err != nil && !lmdb.IsNotFound(err) {
				panic(err)
			}
		},
		"CursorClose": func(cur *lmdb.Cursor) {
			cur.Close()
		},
	} {
		switch fn := fn.(type) {
		case func(context.Context, string, uint32) uint32:
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, p.ctxKeyMeta)
				envID := fn(ctx, string(key(m, meta)), flags(m, meta))
				writeUint32(m, meta.ptrEnv, uint32(envID))
			})
		case func(context.Context, uint32):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, p.ctxKeyMeta)
				fn(ctx, envID(m, meta))
			})
		case func(*lmdb.Env) *lmdb.Stat:
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, p.ctxKeyMeta)
				stat := fn(p.env(ctx, m, meta))
				data, _ := json.Marshal(stat)
				val := append(valBuf(m, meta), data...)
				writeUint32(m, meta.ptrValLen, uint32(len(val)))
			})
		case func(*lmdb.Txn, string, uint32) lmdb.DBI:
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, p.ctxKeyMeta)
				dbi := fn(txn(m, meta), string(key(m, meta)), flags(m, meta))
				writeUint32(m, meta.ptrDbi, uint32(dbi))
			})
		case func(*lmdb.Txn, lmdb.DBI):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, p.ctxKeyMeta)
				fn(txn(m, meta), dbi(m, meta))
			})
		case func(*lmdb.Txn, lmdb.DBI) *lmdb.Stat:
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, p.ctxKeyMeta)
				stat := fn(txn(m, meta), dbi(m, meta))
				data, _ := json.Marshal(stat)
				val := append(valBuf(m, meta), data...)
				writeUint32(m, meta.ptrValLen, uint32(len(val)))
			})
		case func(*lmdb.Txn, lmdb.DBI, []byte, []byte):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, p.ctxKeyMeta)
				fn(txn(m, meta), dbi(m, meta), key(m, meta), val(m, meta))
			})
		case func(*lmdb.Txn, lmdb.DBI, []byte, []byte) []byte:
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, p.ctxKeyMeta)
				val := fn(txn(m, meta), dbi(m, meta), key(m, meta), valBuf(m, meta))
				writeUint32(m, meta.ptrValLen, uint32(len(val)))
			})
		case func(*lmdb.Cursor, uint32):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, p.ctxKeyMeta)
				fn(cur(m, meta), flags(m, meta))
			})
		case func(*lmdb.Cursor):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, p.ctxKeyMeta)
				fn(cur(m, meta))
			})
		case func(*lmdb.Cursor, []byte, []byte, uint32) ([]byte, []byte):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, p.ctxKeyMeta)
				key, val := fn(cur(m, meta), key(m, meta), val(m, meta), flags(m, meta))
				writeUint32(m, meta.ptrKeyLen, uint32(len(key)))
				writeUint32(m, meta.ptrValLen, uint32(len(val)))
			})
		case func(*lmdb.Cursor, []byte, []byte, uint32):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, p.ctxKeyMeta)
				fn(cur(m, meta), key(m, meta), val(m, meta), flags(m, meta))
			})
		case func(*lmdb.Txn, lmdb.DBI) *lmdb.Cursor:
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, p.ctxKeyMeta)
				cur := fn(txn(m, meta), dbi(m, meta))
				meta.curID++
				meta.cursor[meta.curID] = cur
				writeUint32(m, meta.ptrCur, meta.curID)
			})
		case func(*lmdb.Env, *lmdb.Txn, uint32) *lmdb.Txn:
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, p.ctxKeyMeta)
				var parent *lmdb.Txn
				parentID := readUint32(m, meta.ptrTxn)
				if parentID > 0 {
					parent = meta.txn[parentID]
				}
				txn := fn(p.env(ctx, m, meta), parent, flags(m, meta))
				meta.txnID++
				meta.txn[meta.txnID] = txn
				writeUint32(m, meta.ptrTxn, meta.txnID)
			})
		case func(*lmdb.Txn):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, p.ctxKeyMeta)
				fn(txn(m, meta))
				delete(meta.txn, txnID(m, meta))
			})
		default:
			log.Panicf("Method signature implementation missing: %#v", fn)
		}
	}
}

func (p *plugin) ShardClose(ctx context.Context) {
	shardID := get[uint64](ctx, p.ctxKeyTenantID)
	p.Lock()
	defer p.Unlock()
	shard, ok := p.shards[shardID]
	if !ok {
		return
	}
	delete(p.shards, shardID)
	envs := shard.envs
	for _, env := range envs {
		if err := env.Close(); err != nil {
			panic(err)
		}
	}
}

func (p *plugin) ShardDelete(ctx context.Context) {
	if err := os.RemoveAll(get[string](ctx, p.ctxKeyLocalDir)); err != nil {
		panic(err)
	}
	if err := os.RemoveAll(get[string](ctx, p.ctxKeyBlockDir)); err != nil {
		panic(err)
	}
}

func (p *plugin) ShardSync(ctx context.Context) {
	shardID := get[uint64](ctx, p.ctxKeyTenantID)
	p.RLock()
	defer p.RUnlock()
	shard, ok := p.shards[shardID]
	if !ok {
		return
	}
	envs := shard.envs
	for _, env := range envs {
		if err := env.Sync(true); err != nil {
			panic(err)
		}
	}
}

func (p *plugin) Reset(ctx context.Context) {
	meta := get[*meta](ctx, p.ctxKeyMeta)
	for id, txn := range meta.txn {
		txn.Abort()
		delete(meta.txn, id)
	}
	for id, cur := range meta.cursor {
		cur.Close()
		delete(meta.cursor, id)
	}
	meta.curID = 0
	meta.txnID = 0
}

func (p *plugin) Stop() {
	p.RLock()
	defer p.RUnlock()
	for _, shard := range p.shards {
		for _, env := range shard.envs {
			if err := env.Sync(true); err != nil {
				panic(err.Error())
			}
			if err := env.Close(); err != nil {
				panic(err.Error())
			}
		}
	}
	p.shards = map[uint64]*shard{}
}

func (p *plugin) env(ctx context.Context, m api.Module, meta *meta) *lmdb.Env {
	p.RLock()
	defer p.RUnlock()
	shardID := get[uint64](ctx, p.ctxKeyTenantID)
	shard, ok := p.shards[shardID]
	if !ok {
		log.Panicf("Shard not found: %d", shardID)
	}
	envID := envID(m, meta)
	env, ok := shard.envs[envID]
	if !ok {
		log.Panicf("Env not found: %d", envID)
	}
	return env
}

func get[T any](ctx context.Context, key string) T {
	v := ctx.Value(key)
	if v == nil {
		log.Panicf("Context item missing %s", key)
	}
	return v.(T)
}

func key(m api.Module, meta *meta) []byte {
	return read(m, meta.ptrKey, meta.ptrKeyLen, meta.ptrKeyMax)
}

func val(m api.Module, meta *meta) []byte {
	return read(m, meta.ptrVal, meta.ptrValLen, meta.ptrValMax)
}

func keyBuf(m api.Module, meta *meta) []byte {
	return read(m, meta.ptrKey, 0, meta.ptrKeyMax)
}

func valBuf(m api.Module, meta *meta) []byte {
	return read(m, meta.ptrVal, 0, meta.ptrValMax)
}

func envID(m api.Module, meta *meta) uint32 {
	return readUint32(m, meta.ptrEnv)
}

func dbi(m api.Module, meta *meta) lmdb.DBI {
	return lmdb.DBI(readUint32(m, meta.ptrDbi))
}

func flags(m api.Module, meta *meta) uint32 {
	return readUint32(m, meta.ptrFlg)
}

func cur(m api.Module, meta *meta) (cur *lmdb.Cursor) {
	curID := readUint32(m, meta.ptrCur)
	cur, ok := meta.cursor[curID]
	if !ok {
		log.Panicf("Cursor not found: %d", curID)
	}
	return
}

func read(m api.Module, ptrData, ptrLen, ptrMax uint32) (buf []byte) {
	buf, ok := m.Memory().Read(ptrData, readUint32(m, ptrMax))
	if !ok {
		log.Panicf("Memory.Read(%d, %d) out of range", ptrData, ptrLen)
	}
	return buf[:readUint32(m, ptrLen)]
}

func readUint32(m api.Module, ptr uint32) (val uint32) {
	val, ok := m.Memory().ReadUint32Le(ptr)
	if !ok {
		log.Panicf("Memory.Read(%d) out of range", ptr)
	}
	return
}

func writeUint32(m api.Module, ptr uint32, val uint32) {
	if ok := m.Memory().WriteUint32Le(ptr, val); !ok {
		log.Panicf("Memory.Read(%d) out of range", ptr)
	}
}

func txn(m api.Module, meta *meta) *lmdb.Txn {
	return meta.txn[txnID(m, meta)]
}

func txnID(m api.Module, meta *meta) uint32 {
	return readUint32(m, meta.ptrTxn)
}

func beginTxn(env *lmdb.Env, parent *lmdb.Txn, flags uint32) (t *lmdb.Txn) {
	t, err := env.BeginTxn(parent, uint(flags)&txnFlags)
	if err != nil {
		panic(err)
	}
	t.RawRead = true
	return
}
