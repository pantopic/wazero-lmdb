package main

import (
	"errors"
	"strconv"
	"unsafe"

	"github.com/pantopic/wazero-lmdb/lmdb-go"
)

func main() {}

var env *lmdb.Env
var txn *lmdb.Txn
var dbi lmdb.DBI
var cur *lmdb.Cursor
var err error

//export open
func open() {
	env, err = lmdb.Open("test", lmdb.Create)
	if err != nil {
		panic(err)
	}
}

//export delete
func delete() {
	env.Delete()
}

//export stat
func stat() uint64 {
	s, err := env.Stat()
	if err != nil {
		panic(err)
	}
	return sliceToPtr(s.ToBytes())
}

//export begin
func begin() {
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		panic(err)
	}
}

//export beginread
func beginread() {
	txn, err = env.BeginTxn(nil, lmdb.Readonly)
	if err != nil {
		panic(err)
	}
}

//export db
func db() {
	dbi, err = txn.OpenDBI("test", lmdb.Create)
	if err != nil {
		panic(err)
	}
}

//export dbstat
func dbstat() uint64 {
	s, err := txn.Stat(dbi)
	if err != nil {
		panic(err)
	}
	return sliceToPtr(s.ToBytes())
}

//export dbdrop
func dbdrop() {
	txn.Drop(dbi)
}

//export set
func set() {
	txn.Put(dbi, []byte(`a`), []byte(`1`), 0)
}

//export get
func get() {
	v, err := txn.Get(dbi, []byte(`a`))
	if err != nil {
		panic(err)
	}
	if string(v) != `1` {
		panic(`wrong value`)
	}
}

//export getmissing
func getmissing() {
	_, err := txn.Get(dbi, []byte(`ddd`))
	if err == nil {
		panic(`error not returned`)
	}
	if !lmdb.IsNotFound(err) {
		panic(err)
	}
}

//export del
func del() {
	txn.Del(dbi, []byte(`a`), nil)
}

//export commit
func commit() {
	txn.Commit()
}

//export set2
func set2() {
	txn.Put(dbi, []byte(`b`), []byte(`2`), 0)
}

//export get2
func get2() {
	v, err := txn.Get(dbi, []byte(`b`))
	if err != nil {
		panic(err)
	}
	if string(v) != `2` {
		panic(`wrong value`)
	}
}

//export update
func update() {
	if err := env.Update(func(txn *lmdb.Txn) error {
		txn.Put(dbi, []byte(`b`), []byte(`22`), 0)
		return nil
	}); err != nil {
		panic(err)
	}
}

//export updatefail
func updatefail() {
	if err := env.Update(func(txn *lmdb.Txn) error {
		txn.Put(dbi, []byte(`b`), []byte(`222`), 0)
		return errors.New(`I can't believe you've done this.`)
	}); err != nil {
		// panic(err)
	}
}

//export view
func view() {
	env.View(func(txn *lmdb.Txn) (err error) {
		v, err := txn.Get(dbi, []byte(`b`))
		if err != nil {
			return
		}
		if string(v) != `22` {
			err = errors.New("Wrong value: " + string(v) + " != 22")
		}
		return
	})
}

//export stress
func stress(limit uint32) {
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		panic(err)
	}
	if dbi, err = txn.OpenDBI("test", lmdb.Create); err != nil {
		panic(err)
	}
	n := int64(limit)
	for i := range n {
		if err := txn.Put(dbi, []byte(strconv.FormatInt(i+1e15, 16)), []byte(strconv.FormatInt(n-i+1e15, 16)), 0); err != nil {
			panic(err)
		}
	}
	txn.Commit()
}

//export abort
func abort() {
	txn.Abort()
}

//export close
func close() {
	env.Close()
}

//export cursoropen
func cursoropen() {
	if dbi, err = txn.OpenDBI("test", lmdb.Create); err != nil {
		panic(err)
	}
	if cur, err = txn.OpenCursor(dbi); err != nil {
		panic(err)
	}
}

//export cursorfirst
func cursorfirst() {
	k, v := cur.Get(nil, nil, lmdb.First)
	if string(k) != `b` {
		panic(`wrong key: ` + string(k))
	}
	if string(v) != `22` {
		panic(`wrong value: ` + string(v))
	}
}

//export cursorput
func cursorput() {
	cur.Put([]byte(`c`), []byte(`3`), 0)
}

//export cursorcurrent
func cursorcurrent() {
	k, v := cur.Get(nil, nil, lmdb.GetCurrent)
	if string(k) != `c` {
		panic(`wrong key: ` + string(k))
	}
	if string(v) != `3` {
		panic(`wrong value: ` + string(v))
	}
}

//export cursornext
func cursornext() {
	k, v := cur.Get(nil, nil, lmdb.Next)
	if string(k) != `c` {
		panic(`wrong key: ` + string(k))
	}
	if string(v) != `3` {
		panic(`wrong value: ` + string(v))
	}
}

//export cursordel
func cursordel() {
	cur.Del(lmdb.Current)
}

//export cursorclose
func cursorclose() {
	cur.Close()
}

func sliceToPtr(b []byte) uint64 {
	return uint64(uintptr(unsafe.Pointer(&b[0])))<<32 + uint64(len(b))
}

// Fix for lint rule `unusedfunc`
var _ = open
var _ = stat
var _ = begin
var _ = db
var _ = dbstat
var _ = set
var _ = set2
var _ = commit
var _ = abort
var _ = close
var _ = get
var _ = get2
var _ = beginread
var _ = stress
var _ = dbdrop
var _ = del
var _ = cursoropen
var _ = cursorfirst
var _ = cursorput
var _ = cursornext
var _ = cursorcurrent
var _ = cursorclose
var _ = cursordel
var _ = view
var _ = update
var _ = updatefail
var _ = delete
var _ = getmissing
