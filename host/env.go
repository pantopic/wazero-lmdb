package wazero_lmdb

import (
	"context"
	"os"

	"github.com/PowerDNS/lmdb-go/lmdb"
)

func EnvCreate(dir string) *lmdb.Env {
	err := os.MkdirAll(dir, 0700)
	if err != nil {
		panic(err)
	}
	env, err := lmdb.NewEnv()
	if err != nil {
		panic(err)
	}
	env.SetMaxDBs(255)
	env.SetMapSize(int64(64 << 30)) // 64 GiB
	env.SetMaxReaders(1 << 16)      // 64k readers
	if err = env.Open(dir+`/data.mdb`, uint(lmdb.NoMemInit|lmdb.NoSync|lmdb.NoMetaSync|lmdb.NoSubdir|lmdb.Create), 0700); err != nil {
		panic(err)
	}
	return env
}

func EnvRegister(ctx context.Context, env *lmdb.Env) context.Context {
	return context.WithValue(ctx, ctxKeyEnv, env)
}
