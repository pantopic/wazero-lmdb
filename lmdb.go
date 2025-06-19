package plugin_lmdb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/PowerDNS/lmdb-go/lmdb"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

var (
	ctxKeyMeta    = `pantopic_plugin_lmdb_meta`
	ctxKeyShardID = `pantopic_shard_id`
	optEnv        = lmdb.NoMemInit | lmdb.NoReadahead | lmdb.NoSync | lmdb.NoMetaSync
)

type meta struct {
	keyMax uint32
	valMax uint32
	keyLen uint32
	valLen uint32
	ptrEnv uint32
	ptrTxn uint32
	ptrDbi uint32
	ptrCur uint32
	ptrKey uint32
	ptrVal uint32
	txn    map[uint32]*lmdb.Txn
	cursor map[uint32]*lmdb.Cursor
	filter map[uint32]*filter
	txnID  uint32
	curID  uint32
}

type filter struct {
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

	shards  map[uint64]*shard
	dataDir string
}

func New(dataDir string) *plugin {
	return &plugin{
		dataDir: dataDir,
		shards:  map[uint64]*shard{},
	}
}

func (p *plugin) InitContext(ctx context.Context, m api.Module) context.Context {
	lmdb := m.ExportedFunction(`lmdb`)
	stack, err := lmdb.Call(ctx)
	if err != nil {
		panic(err)
	}
	ptr := uint32(stack[0])
	meta := &meta{}
	meta.keyMax, _ = m.Memory().ReadUint32Le(ptr)
	meta.valMax, _ = m.Memory().ReadUint32Le(ptr + 4)
	meta.keyLen, _ = m.Memory().ReadUint32Le(ptr + 8)
	meta.valLen, _ = m.Memory().ReadUint32Le(ptr + 12)
	meta.ptrEnv, _ = m.Memory().ReadUint32Le(ptr + 16)
	meta.ptrTxn, _ = m.Memory().ReadUint32Le(ptr + 20)
	meta.ptrDbi, _ = m.Memory().ReadUint32Le(ptr + 24)
	meta.ptrCur, _ = m.Memory().ReadUint32Le(ptr + 28)
	meta.ptrKey, _ = m.Memory().ReadUint32Le(ptr + 32)
	meta.ptrVal, _ = m.Memory().ReadUint32Le(ptr + 36)
	return context.WithValue(ctx, ctxKeyMeta, meta)
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
		"EnvOpen": func(ctx context.Context, name string) uint32 {
			p.Lock()
			shardID := get[uint64](ctx, ctxKeyShardID)
			shard, ok := p.shards[shardID]
			if !ok {
				shard = newShard()
				p.shards[shardID] = shard
			}
			p.Unlock()
			path := fmt.Sprintf(`%s/%016x/%s.mdb`, p.dataDir, shardID, name)
			if err := os.MkdirAll(path, 0777); err != nil {
				panic(err)
			}
			env, err := lmdb.NewEnv()
			if err != nil {
				panic(err)
			}
			env.SetMaxDBs(1 << 32)
			env.SetMapSize(int64(1 << 37))
			err = env.Open(path, uint(optEnv), 0700)
			if err != nil {
				panic(err)
			}
			shard.Lock()
			defer shard.Unlock()
			shard.envID++
			shard.envs[shard.envID] = env
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
			shardID := get[uint64](ctx, ctxKeyShardID)
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
		"EnvDelete": func(ctx context.Context, name string) {
			path := fmt.Sprintf(`%s/%08x/%s.mdb`, p.dataDir, get[uint64](ctx, ctxKeyShardID), name)
			if err := os.Remove(path); err != nil {
				panic(err)
			}
		},
		"Begin": func(env *lmdb.Env, parent *lmdb.Txn, readonly bool) *lmdb.Txn {
			return beginTxn(env, parent, readonly)
		},
		"Commit": func(txn *lmdb.Txn) {
			txn.Commit()
		},
		"Abort": func(txn *lmdb.Txn) {
			txn.Abort()
		},
		"DbCreate": func(txn *lmdb.Txn, name string, flags uint32) (dbi lmdb.DBI) {
			dbi, err := txn.OpenDBI(name, uint(lmdb.Create))
			if err != nil {
				panic(err)
			}
			return
		},
		"DbOpen": func(txn *lmdb.Txn, name string, flags uint32) (dbi lmdb.DBI) {
			dbi, err := txn.OpenDBI(name, 0)
			if err != nil && !lmdb.IsNotFound(err) {
				panic(err)
			}
			return
		},
		"DbDrop": func(txn *lmdb.Txn, dbi lmdb.DBI) {
			err := txn.Drop(dbi, true)
			if err != nil && !lmdb.IsNotFound(err) {
				panic(err)
			}
			return
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
			return append(val, v...)
		},
		"Del": func(txn *lmdb.Txn, dbi lmdb.DBI, key []byte) {
			err := txn.Del(dbi, key, nil)
			if err != nil && !lmdb.IsNotFound(err) {
				panic(err)
			}
			return
		},
		"DelDup": func(txn *lmdb.Txn, dbi lmdb.DBI, key, val []byte) {
			err := txn.Del(dbi, key, val)
			if err != nil && !lmdb.IsNotFound(err) {
				panic(err)
			}
			return
		},
		"CursorOpen": func(txn *lmdb.Txn, dbi lmdb.DBI, key, filter []byte) *lmdb.Cursor {
			cur, err := txn.OpenCursor(dbi)
			if err != nil {
				panic(err)
			}
			if len(key) > 0 {
				_, _, err := cur.Get(key, nil, lmdb.SetRange)
				if err != nil {
					panic(err)
				}
			}
			return cur
		},
		"CursorSeek": func(cur *lmdb.Cursor, key []byte) {
			_, _, err := cur.Get(key, nil, lmdb.SetRange)
			if err != nil {
				panic(err)
			}
		},
		"CursorGet": func(cur *lmdb.Cursor, key, val []byte) []byte {
			_, val, err := cur.Get(key, val, 0)
			if err != nil && !lmdb.IsNotFound(err) {
				panic(err)
			}
			return val
		},
		"CursorDel": func(cur *lmdb.Cursor) {
			err := cur.Del(0)
			if err != nil && !lmdb.IsNotFound(err) {
				panic(err)
			}
		},
		"CursorNext": func(cur *lmdb.Cursor, key, val []byte) ([]byte, []byte) {
			key, val, err := cur.Get(key, val, lmdb.Next)
			if err != nil && !lmdb.IsNotFound(err) {
				panic(err)
			}
			return key, val
		},
		"CursorNextDup": func(cur *lmdb.Cursor, key, val []byte) ([]byte, []byte) {
			key, val, err := cur.Get(key, val, lmdb.NextDup)
			if err != nil && !lmdb.IsNotFound(err) {
				panic(err)
			}
			return key, val
		},
		"CursorNextNoDup": func(cur *lmdb.Cursor, key, val []byte) ([]byte, []byte) {
			key, val, err := cur.Get(key, val, lmdb.NextNoDup)
			if err != nil && !lmdb.IsNotFound(err) {
				panic(err)
			}
			return key, val
		},
		"CursorPrev": func(cur *lmdb.Cursor, key, val []byte) ([]byte, []byte) {
			key, val, err := cur.Get(key, val, lmdb.Prev)
			if err != nil && !lmdb.IsNotFound(err) {
				panic(err)
			}
			return key, val
		},
		"CursorPrevDup": func(cur *lmdb.Cursor, key, val []byte) ([]byte, []byte) {
			key, val, err := cur.Get(key, val, lmdb.PrevDup)
			if err != nil && !lmdb.IsNotFound(err) {
				panic(err)
			}
			return key, val
		},
		"CursorPrevNoDup": func(cur *lmdb.Cursor, key, val []byte) ([]byte, []byte) {
			key, val, err := cur.Get(key, val, lmdb.PrevNoDup)
			if err != nil && !lmdb.IsNotFound(err) {
				panic(err)
			}
			return key, val
		},
	} {
		switch fn.(type) {
		case func(context.Context, string) uint32:
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				envID := fn.(func(context.Context, string) uint32)(ctx, string(key(m, meta)))
				writeUint32(m, meta.ptrEnv, uint32(envID))
			})
		case func(context.Context, string):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				fn.(func(context.Context, string))(ctx, string(key(m, meta)))
			})
		case func(context.Context, uint32):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				fn.(func(context.Context, uint32))(ctx, envID(m, meta))
			})
		case func(*lmdb.Env) *lmdb.Stat:
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				stat := fn.(func(*lmdb.Env) *lmdb.Stat)(p.env(ctx, m, meta))
				data, _ := json.Marshal(stat)
				val := append(valBuf(m, meta), data...)
				writeUint32(m, meta.valLen, uint32(len(val)))
			})
		case func(*lmdb.Txn, string, uint32) lmdb.DBI:
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				dbi := fn.(func(*lmdb.Txn, string, uint32) lmdb.DBI)(txn(m, meta), string(key(m, meta)), uint32(stack[0]))
				writeUint32(m, meta.ptrDbi, uint32(dbi))
			})
		case func(*lmdb.Txn, lmdb.DBI):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				fn.(func(*lmdb.Txn, lmdb.DBI))(txn(m, meta), dbi(m, meta))
			})
		case func(*lmdb.Txn, lmdb.DBI, []byte):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				fn.(func(*lmdb.Txn, lmdb.DBI, []byte))(txn(m, meta), dbi(m, meta), key(m, meta))
			})
		case func(*lmdb.Txn, lmdb.DBI, []byte, []byte):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				fn.(func(*lmdb.Txn, lmdb.DBI, []byte, []byte))(txn(m, meta), dbi(m, meta), key(m, meta), val(m, meta))
			})
		case func(*lmdb.Txn, lmdb.DBI, []byte, []byte) []byte:
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				val := fn.(func(*lmdb.Txn, lmdb.DBI, []byte, []byte) []byte)(txn(m, meta), dbi(m, meta), key(m, meta), valBuf(m, meta))
				writeUint32(m, meta.keyLen, uint32(len(val)))
			})
		case func(*lmdb.Txn, lmdb.DBI, []byte, []byte) ([]byte, []byte):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				key, val := fn.(func(*lmdb.Txn, lmdb.DBI, []byte, []byte) ([]byte, []byte))(txn(m, meta), dbi(m, meta), keyBuf(m, meta), valBuf(m, meta))
				writeUint32(m, meta.keyLen, uint32(len(key)))
				writeUint32(m, meta.valLen, uint32(len(val)))
			})
		case func(*lmdb.Cursor):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				fn.(func(*lmdb.Cursor))(cur(m, meta))
			})
		case func(*lmdb.Cursor, []byte):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				fn.(func(*lmdb.Cursor, []byte))(cur(m, meta), key(m, meta))
			})
		case func(*lmdb.Cursor, []byte) []byte:
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				val := fn.(func(*lmdb.Cursor, []byte) []byte)(cur(m, meta), key(m, meta))
				writeUint32(m, meta.valLen, uint32(len(val)))
			})
		case func(*lmdb.Cursor, []byte, []byte) []byte:
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				val := fn.(func(*lmdb.Cursor, []byte, []byte) []byte)(cur(m, meta), key(m, meta), valBuf(m, meta))
				writeUint32(m, meta.valLen, uint32(len(val)))
			})
		case func(*lmdb.Cursor, []byte, []byte) ([]byte, []byte):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				key, val := fn.(func(*lmdb.Cursor, []byte, []byte) ([]byte, []byte))(cur(m, meta), key(m, meta), val(m, meta))
				writeUint32(m, meta.keyLen, uint32(len(key)))
				writeUint32(m, meta.valLen, uint32(len(val)))
			})
		case func(*lmdb.Txn, lmdb.DBI, []byte, []byte) *lmdb.Cursor:
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				cur := fn.(func(*lmdb.Txn, lmdb.DBI, []byte, []byte) *lmdb.Cursor)(txn(m, meta), dbi(m, meta), key(m, meta), val(m, meta))
				meta.curID++
				meta.cursor[meta.curID] = cur
				writeUint32(m, meta.ptrCur, meta.curID)
			})
		case func(*lmdb.Env, *lmdb.Txn, bool) *lmdb.Txn:
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				var parent *lmdb.Txn
				parentID := readUint32(m, meta.ptrTxn)
				if parentID > 0 {
					parent = meta.txn[parentID]
				}
				txn := fn.(func(*lmdb.Env, *lmdb.Txn, bool) *lmdb.Txn)(p.env(ctx, m, meta), parent, dbi(m, meta) > 0)
				meta.txnID++
				meta.txn[meta.txnID] = txn
				writeUint32(m, meta.ptrTxn, meta.txnID)
			})
		case func(*lmdb.Txn):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				fn.(func(*lmdb.Txn) *lmdb.Txn)(txn(m, meta))
				delete(meta.txn, txnID(m, meta))
			})
		default:
			log.Panicf("Method signature implementation missing: %#v", fn)
		}
	}
}
func (p *plugin) ShardClose(shardID uint64) {
	p.Lock()
	envs := p.shards[shardID].envs
	delete(p.shards, shardID)
	p.Unlock()
	for _, env := range envs {
		if err := env.Close(); err != nil {
			panic(err)
		}
	}
}

func (p *plugin) ShardDelete(shardID uint64) {
	path := fmt.Sprintf(`%s/%08x`, p.dataDir, shardID)
	if err := os.RemoveAll(path); err != nil {
		panic(err)
	}
}

func (p *plugin) ShardSync(shardID uint64) {
	p.RLock()
	envs := p.shards[shardID].envs
	p.RUnlock()
	for _, env := range envs {
		if err := env.Sync(true); err != nil {
			panic(err)
		}
	}
}

func (p *plugin) Reset(ctx context.Context) {
	meta := get[*meta](ctx, ctxKeyMeta)
	for id, txn := range meta.txn {
		txn.Abort()
		delete(meta.txn, id)
	}
	for id, cur := range meta.cursor {
		cur.Close()
		delete(meta.cursor, id)
	}
	clear(meta.filter)
	meta.curID = 0
	meta.txnID = 0
}

func (p *plugin) Stop() {
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
	shardID := get[uint64](ctx, ctxKeyShardID)
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
	return read(m, meta.keyLen, meta.ptrKey)
}

func val(m api.Module, meta *meta) []byte {
	return read(m, meta.valLen, meta.ptrVal)
}

func keyBuf(m api.Module, meta *meta) []byte {
	return read(m, meta.keyMax, meta.ptrKey)[:0]
}

func valBuf(m api.Module, meta *meta) []byte {
	return read(m, meta.valMax, meta.ptrVal)[:0]
}

func envID(m api.Module, meta *meta) uint32 {
	return readUint32(m, meta.ptrEnv)
}

func dbi(m api.Module, meta *meta) lmdb.DBI {
	return lmdb.DBI(readUint32(m, meta.ptrDbi))
}

func cur(m api.Module, meta *meta) (cur *lmdb.Cursor) {
	curID := readUint32(m, meta.ptrCur)
	cur, ok := meta.cursor[curID]
	if !ok {
		log.Panicf("Cursor not found: %d", curID)
	}
	return
}

func read(m api.Module, ptrLen, ptrData uint32) (buf []byte) {
	buf, ok := m.Memory().Read(ptrData, readUint32(m, ptrLen))
	if !ok {
		log.Panicf("Memory.Read(%d, %d) out of range", ptrData, ptrLen)
	}
	return
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
	return
}

func txn(m api.Module, meta *meta) *lmdb.Txn {
	return meta.txn[txnID(m, meta)]
}

func txnID(m api.Module, meta *meta) uint32 {
	return readUint32(m, meta.ptrTxn)
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
