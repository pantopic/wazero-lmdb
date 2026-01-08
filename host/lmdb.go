package wazero_lmdb

import (
	"context"
	"encoding/binary"
	"log"
	"runtime"
	"sync"

	"github.com/PowerDNS/lmdb-go/lmdb"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// Name is the name of this host module.
const Name = "pantopic/wazero-lmdb"

var (
	DefaultCtxKeyMeta = `__wazero_lmdb_meta_key`
	DefaultCtxKeyEnv  = `__wazero_lmdb_env`

	dbFlagMask     uint = lmdb.Create | lmdb.DupSort
	txnFlagMask    uint = lmdb.Readonly
	txnPutFlagMask uint = lmdb.NoDupData | lmdb.NoOverwrite | lmdb.Append | lmdb.AppendDup
	curPutFlagMask uint = lmdb.NoDupData | lmdb.NoOverwrite | lmdb.Append | lmdb.AppendDup | lmdb.Current
	curGetFlagMask uint = 1<<18 - 1
)

type meta struct {
	ptrKeyCap uint32
	ptrKeyLen uint32
	ptrKey    uint32
	ptrValCap uint32
	ptrValLen uint32
	ptrVal    uint32
	ptrTxn    uint32
	ptrDbi    uint32
	ptrCur    uint32
	ptrFlg    uint32
	ptrErr    uint32
	txn       map[uint32]*lmdb.Txn
	cursor    map[uint32]*lmdb.Cursor
	txnID     uint32
	curID     uint32
}

type hostModule struct {
	sync.RWMutex

	module     api.Module
	ctxKeyMeta string
	ctxKeyEnv  string
}

func New(opts ...Option) *hostModule {
	p := &hostModule{
		ctxKeyEnv: DefaultCtxKeyEnv,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (h *hostModule) Name() string {
	return Name
}

func (h *hostModule) InitContext(ctx context.Context, m api.Module) (context.Context, error) {
	stack, err := m.ExportedFunction(`__lmdb`).Call(ctx)
	if err != nil {
		return ctx, err
	}
	meta := &meta{
		txn:    make(map[uint32]*lmdb.Txn),
		cursor: make(map[uint32]*lmdb.Cursor),
	}
	ptr := uint32(stack[0])
	for i, v := range []*uint32{
		&meta.ptrKeyCap,
		&meta.ptrKeyLen,
		&meta.ptrKey,
		&meta.ptrValCap,
		&meta.ptrValLen,
		&meta.ptrVal,
		&meta.ptrTxn,
		&meta.ptrDbi,
		&meta.ptrCur,
		&meta.ptrFlg,
		&meta.ptrErr,
	} {
		*v = readUint32(m, ptr+uint32(4*i))
	}
	return context.WithValue(ctx, h.ctxKeyMeta, meta), nil
}

func (h *hostModule) ContextCopy(dst, src context.Context) context.Context {
	dst = context.WithValue(dst, h.ctxKeyMeta, get[*meta](src, h.ctxKeyMeta))
	return dst
}

func (h *hostModule) Register(ctx context.Context, r wazero.Runtime) (err error) {
	builder := r.NewHostModuleBuilder(Name)
	register := func(name string, fn func(ctx context.Context, m api.Module, stack []uint64)) {
		builder = builder.NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(fn), nil, nil).Export(name)
	}
	for name, fn := range map[string]any{
		"__lmdb_begin": func(env *lmdb.Env, parent *lmdb.Txn, flags uint32) (txn *lmdb.Txn, err error) {
			if flags&lmdb.Readonly == 0 {
				runtime.LockOSThread()
			}
			return beginTxn(env, parent, flags)
		},
		"__lmdb_commit": func(txn *lmdb.Txn) error {
			defer runtime.UnlockOSThread()
			return txn.Commit()
		},
		"__lmdb_abort": func(txn *lmdb.Txn) {
			defer runtime.UnlockOSThread()
			txn.Abort()
		},
		"__lmdb_db_open": func(txn *lmdb.Txn, name string, flags uint32) (dbi lmdb.DBI, err error) {
			return txn.OpenDBI(name, uint(flags)&dbFlagMask)
		},
		"__lmdb_db_stat": func(txn *lmdb.Txn, dbi lmdb.DBI) (stat *lmdb.Stat, err error) {
			return txn.Stat(dbi)
		},
		"__lmdb_db_drop": func(txn *lmdb.Txn, dbi lmdb.DBI) (err error) {
			return txn.Drop(dbi, true)
		},
		"__lmdb_put": func(txn *lmdb.Txn, dbi lmdb.DBI, key, val []byte, flags uint32) (err error) {
			return txn.Put(dbi, key, val, uint(flags)&txnPutFlagMask)
		},
		"__lmdb_get": func(txn *lmdb.Txn, dbi lmdb.DBI, key, val []byte) ([]byte, error) {
			v, err := txn.Get(dbi, key)
			if err != nil {
				return nil, err
			}
			val = val[:len(v)]
			copy(val, v)
			return val, nil
		},
		"__lmdb_del": func(txn *lmdb.Txn, dbi lmdb.DBI, key, val []byte) (err error) {
			return txn.Del(dbi, key, val)
		},
		"__lmdb_cursor_open": func(txn *lmdb.Txn, dbi lmdb.DBI) (cur *lmdb.Cursor, err error) {
			return txn.OpenCursor(dbi)
		},
		"__lmdb_cursor_get": func(cur *lmdb.Cursor, key, val []byte, flags uint32) ([]byte, []byte, error) {
			k, v, err := cur.Get(key, val, uint(flags)&curGetFlagMask)
			if err != nil {
				return nil, nil, err
			}
			key = key[:len(k)]
			copy(key, k)
			val = val[:len(v)]
			copy(val, v)
			return key, val, nil
		},
		"__lmdb_cursor_del": func(cur *lmdb.Cursor, flags uint32) error {
			return cur.Del(uint(flags))
		},
		"__lmdb_cursor_put": func(cur *lmdb.Cursor, key, val []byte, flags uint32) error {
			return cur.Put(key, val, uint(flags)&curPutFlagMask)
		},
		"__lmdb_cursor_close": func(cur *lmdb.Cursor) {
			cur.Close()
		},
	} {
		switch fn := fn.(type) {
		case func(*lmdb.Env) (*lmdb.Stat, error):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, h.ctxKeyMeta)
				stat, err := fn(h.env(ctx))
				if writeError(m, meta, err) {
					return
				}
				val := append(valBuf(m, meta), statToBytes(stat)...)
				writeUint32(m, meta.ptrValLen, uint32(len(val)))
			})
		case func(*lmdb.Env) error:
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, h.ctxKeyMeta)
				err := fn(h.env(ctx))
				writeError(m, meta, err)
			})
		case func(*lmdb.Txn, string, uint32) (lmdb.DBI, error):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, h.ctxKeyMeta)
				dbi, err := fn(txn(m, meta), string(key(m, meta)), flags(m, meta))
				if writeError(m, meta, err) {
					return
				}
				writeUint32(m, meta.ptrDbi, uint32(dbi))
			})
		case func(*lmdb.Txn, lmdb.DBI) error:
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, h.ctxKeyMeta)
				err := fn(txn(m, meta), dbi(m, meta))
				writeError(m, meta, err)
			})
		case func(*lmdb.Txn, lmdb.DBI) (*lmdb.Stat, error):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, h.ctxKeyMeta)
				stat, err := fn(txn(m, meta), dbi(m, meta))
				if writeError(m, meta, err) {
					return
				}
				val := append(valBuf(m, meta), statToBytes(stat)...)
				writeUint32(m, meta.ptrValLen, uint32(len(val)))
			})
		case func(*lmdb.Txn, lmdb.DBI, []byte, []byte) error:
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, h.ctxKeyMeta)
				err := fn(txn(m, meta), dbi(m, meta), key(m, meta), val(m, meta))
				writeError(m, meta, err)
			})
		case func(*lmdb.Txn, lmdb.DBI, []byte, []byte, uint32) error:
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, h.ctxKeyMeta)
				err := fn(txn(m, meta), dbi(m, meta), key(m, meta), val(m, meta), flags(m, meta))
				writeError(m, meta, err)
			})
		case func(*lmdb.Txn, lmdb.DBI, []byte, []byte) ([]byte, error):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, h.ctxKeyMeta)
				val, err := fn(txn(m, meta), dbi(m, meta), key(m, meta), valBuf(m, meta))
				if writeError(m, meta, err) {
					return
				}
				writeUint32(m, meta.ptrValLen, uint32(len(val)))
			})
		case func(*lmdb.Cursor, uint32) error:
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, h.ctxKeyMeta)
				err := fn(cur(m, meta), flags(m, meta))
				writeError(m, meta, err)
			})
		case func(*lmdb.Cursor):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, h.ctxKeyMeta)
				fn(cur(m, meta))
			})
		case func(*lmdb.Cursor, []byte, []byte, uint32) ([]byte, []byte, error):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, h.ctxKeyMeta)
				key, val, err := fn(cur(m, meta), key(m, meta), val(m, meta), flags(m, meta))
				if writeError(m, meta, err) {
					return
				}
				writeUint32(m, meta.ptrKeyLen, uint32(len(key)))
				writeUint32(m, meta.ptrValLen, uint32(len(val)))
			})
		case func(*lmdb.Cursor, []byte, []byte, uint32) error:
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, h.ctxKeyMeta)
				err := fn(cur(m, meta), key(m, meta), val(m, meta), flags(m, meta))
				writeError(m, meta, err)
			})
		case func(*lmdb.Txn, lmdb.DBI) (*lmdb.Cursor, error):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, h.ctxKeyMeta)
				cur, err := fn(txn(m, meta), dbi(m, meta))
				if writeError(m, meta, err) {
					return
				}
				meta.curID++
				meta.cursor[meta.curID] = cur
				writeUint32(m, meta.ptrCur, meta.curID)
			})
		case func(*lmdb.Env, *lmdb.Txn, uint32) (*lmdb.Txn, error):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, h.ctxKeyMeta)
				var parent *lmdb.Txn
				parentID := readUint32(m, meta.ptrTxn)
				if parentID > 0 {
					parent = meta.txn[parentID]
				}
				txn, err := fn(h.env(ctx), parent, flags(m, meta))
				if writeError(m, meta, err) {
					return
				}
				meta.txnID++
				meta.txn[meta.txnID] = txn
				writeUint32(m, meta.ptrTxn, meta.txnID)
			})
		case func(*lmdb.Txn):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, h.ctxKeyMeta)
				fn(txn(m, meta))
				delete(meta.txn, txnID(m, meta))
			})
		case func(*lmdb.Txn) error:
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, h.ctxKeyMeta)
				err := fn(txn(m, meta))
				delete(meta.txn, txnID(m, meta))
				writeError(m, meta, err)
			})
		default:
			log.Panicf("Method signature implementation missing: %#v", fn)
		}
	}
	h.module, err = builder.Instantiate(ctx)
	return
}

func (h *hostModule) Reset(ctx context.Context) {
	meta := get[*meta](ctx, h.ctxKeyMeta)
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

func (h *hostModule) Stop() (err error) {
	return
}

func (h *hostModule) env(ctx context.Context) *lmdb.Env {
	return get[*lmdb.Env](ctx, h.ctxKeyEnv)
}

func get[T any](ctx context.Context, key string) T {
	v := ctx.Value(key)
	if v == nil {
		log.Panicf("Context item missing %s", key)
	}
	return v.(T)
}

func key(m api.Module, meta *meta) []byte {
	return read(m, meta.ptrKey, meta.ptrKeyLen, meta.ptrKeyCap)
}

func val(m api.Module, meta *meta) []byte {
	return read(m, meta.ptrVal, meta.ptrValLen, meta.ptrValCap)
}

func keyBuf(m api.Module, meta *meta) []byte {
	return read(m, meta.ptrKey, 0, meta.ptrKeyCap)
}

func valBuf(m api.Module, meta *meta) []byte {
	return read(m, meta.ptrVal, 0, meta.ptrValCap)
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

func read(m api.Module, ptrData, ptrLen, ptrCap uint32) (buf []byte) {
	buf, ok := m.Memory().Read(ptrData, readUint32(m, ptrCap))
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

func beginTxn(env *lmdb.Env, parent *lmdb.Txn, flags uint32) (txn *lmdb.Txn, err error) {
	txn, err = env.BeginTxn(parent, uint(flags)&txnFlagMask)
	if err != nil {
		return
	}
	txn.RawRead = true
	return
}

func statToBytes(s *lmdb.Stat) []byte {
	b := make([]byte, 48)
	binary.LittleEndian.PutUint64(b[0:8], uint64(s.PSize))
	binary.LittleEndian.PutUint64(b[8:16], uint64(s.Depth))
	binary.LittleEndian.PutUint64(b[16:24], uint64(s.BranchPages))
	binary.LittleEndian.PutUint64(b[24:32], uint64(s.LeafPages))
	binary.LittleEndian.PutUint64(b[32:40], uint64(s.OverflowPages))
	binary.LittleEndian.PutUint64(b[40:48], uint64(s.Entries))
	return b
}

func statFromBytes(b []byte) *lmdb.Stat {
	return &lmdb.Stat{
		PSize:         uint(binary.LittleEndian.Uint64(b[0:8])),
		Depth:         uint(binary.LittleEndian.Uint64(b[8:16])),
		BranchPages:   binary.LittleEndian.Uint64(b[16:24]),
		LeafPages:     binary.LittleEndian.Uint64(b[24:32]),
		OverflowPages: binary.LittleEndian.Uint64(b[32:40]),
		Entries:       binary.LittleEndian.Uint64(b[40:48]),
	}
}
