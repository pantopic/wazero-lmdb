package lmdb

import (
	"encoding/binary"
)

// LMDB multiuse flags
// See https://pkg.go.dev/github.com/PowerDNS/lmdb-go/lmdb#pkg-constants
const (
	DupSort     uint32 = 0x00004
	Current     uint32 = 0x00040
	Readonly    uint32 = 0x20000
	Create      uint32 = 0x40000
	NoReadahead uint32 = 0x800000

	NoDupData   uint32 = 0x20    // Store the key-value pair only if key is not present (DupSort).
	NoOverwrite uint32 = 0x10    // Store a new key-value pair only if key is not present.
	Append      uint32 = 0x20000 // Append an item to the database.
	AppendDup   uint32 = 0x40000 // Append an item to the database (DupSort).
)

// LMDB cursor flags
const (
	First uint32 = iota
	FirstDup
	GetBoth
	GetBothRange
	GetCurrent
	GetMultiple
	Last
	LastDup
	Next
	NextDup
	NextMultiple
	NextNoDup
	Prev
	PrevDup
	PrevNoDup
	Set
	SetKey
	SetRange
)

type DBI uint32

type lmdbError struct {
	code uint32
	msg  []byte
}

func (err lmdbError) Error() string {
	return string(err.msg)
}

// Env represents an LMDB environment (database file)
// See https://pkg.go.dev/github.com/PowerDNS/lmdb-go/lmdb#Env
type Env struct {
	id uint32
}

func Open(name string, flags uint32) (env *Env, err error) {
	setKey([]byte(name))
	expFlg = flags
	lmdbEnvOpen()
	if errCode > 0 {
		err = lmdbError{errCode, getVal()}
	} else {
		env = &Env{envID}
	}
	return
}

func (e *Env) Stat() (s *Stat, err error) {
	envID = e.id
	lmdbEnvStat()
	if errCode > 0 {
		err = lmdbError{errCode, getVal()}
	} else {
		s = stat.from(getVal())
	}
	return
}

func (e *Env) Close() {
	envID = e.id
	lmdbEnvClose()
}

func (e *Env) Delete() {
	envID = e.id
	lmdbEnvDelete()
}

func (e *Env) BeginTxn(parent *Txn, flags uint32) *Txn {
	envID = e.id
	if parent != nil {
		txnID = parent.id
	} else {
		txnID = 0
	}
	expFlg = flags
	lmdbBegin()
	return &Txn{txnID}
}

func (e *Env) View(fn func(*Txn)) {
	txn := e.BeginTxn(nil, Readonly)
	fn(txn)
	txn.Abort()
}

func (e *Env) Update(fn func(*Txn) error) (err error) {
	txn := e.BeginTxn(nil, 0)
	if err = fn(txn); err == nil {
		txn.Commit()
	} else {
		txn.Abort()
	}
	return
}

// Txn represents an LMDB transaction
// See https://pkg.go.dev/github.com/PowerDNS/lmdb-go/lmdb#Txn
type Txn struct {
	id uint32
}

func (t *Txn) OpenDBI(name string, flags uint32) DBI {
	txnID = t.id
	expFlg = flags
	setKey([]byte(name))
	lmdbDbOpen()
	return expDbi
}

func (t *Txn) Drop(dbi DBI) {
	txnID = t.id
	expDbi = dbi
	lmdbDbDrop()
}

func (t *Txn) Stat(dbi DBI) (s *Stat, err error) {
	txnID = t.id
	expDbi = dbi
	lmdbDbStat()
	if errCode > 0 {
		err = lmdbError{errCode, getVal()}
	} else {
		s = stat.from(getVal())
	}
	return
}

func (t *Txn) Put(dbi DBI, key, val []byte, flags uint32) {
	txnID = t.id
	expDbi = dbi
	expFlg = flags
	setKey(key)
	setVal(val)
	lmdbPut()
}

func (t *Txn) Get(dbi DBI, key []byte) []byte {
	txnID = t.id
	expDbi = dbi
	setKey(key)
	lmdbGet()
	return getVal()
}

func (t *Txn) Del(dbi DBI, key, val []byte) {
	txnID = t.id
	expDbi = dbi
	setKey(key)
	setVal(val)
	lmdbDel()
}

func (t *Txn) OpenCursor(dbi DBI) *Cursor {
	txnID = t.id
	expDbi = dbi
	lmdbCursorOpen()
	return &Cursor{curID}
}

func (t *Txn) Commit() {
	txnID = t.id
	lmdbCommit()
}

func (t *Txn) Abort() {
	txnID = t.id
	lmdbAbort()
}

// Cursor represents an LMDB cursor
// See https://pkg.go.dev/github.com/PowerDNS/lmdb-go/lmdb#Cursor
type Cursor struct {
	id uint32
}

func (c *Cursor) Get(key, val []byte, flags uint32) ([]byte, []byte) {
	curID = c.id
	expFlg = flags
	setKey(key)
	setVal(val)
	lmdbCursorGet()
	return getKey(), getVal()
}

func (c *Cursor) Put(key, val []byte, flags uint32) {
	curID = c.id
	expFlg = flags
	setKey(key)
	setVal(val)
	lmdbCursorPut()
}

func (c *Cursor) Del(flags uint32) {
	curID = c.id
	expFlg = flags
	lmdbCursorDel()
}

func (c *Cursor) Close() {
	curID = c.id
	lmdbCursorClose()
}

var stat = new(Stat)

type Stat struct {
	PSize         uint   // Size of a database page. This is currently the same for all databases.
	Depth         uint   // Depth (height) of the B-tree
	BranchPages   uint64 // Number of internal (non-leaf) pages
	LeafPages     uint64 // Number of leaf pages
	OverflowPages uint64 // Number of overflow pages
	Entries       uint64 // Number of data items
}

func (s *Stat) from(b []byte) *Stat {
	s.PSize = uint(binary.LittleEndian.Uint64(b[0:8]))
	s.Depth = uint(binary.LittleEndian.Uint64(b[8:16]))
	s.BranchPages = binary.LittleEndian.Uint64(b[16:24])
	s.LeafPages = binary.LittleEndian.Uint64(b[24:32])
	s.OverflowPages = binary.LittleEndian.Uint64(b[32:40])
	s.Entries = binary.LittleEndian.Uint64(b[40:48])
	return s
}

func (s *Stat) ToBytes() []byte {
	b := make([]byte, 48)
	binary.LittleEndian.PutUint64(b[0:8], uint64(s.PSize))
	binary.LittleEndian.PutUint64(b[8:16], uint64(s.Depth))
	binary.LittleEndian.PutUint64(b[16:24], uint64(s.BranchPages))
	binary.LittleEndian.PutUint64(b[24:32], uint64(s.LeafPages))
	binary.LittleEndian.PutUint64(b[32:40], uint64(s.OverflowPages))
	binary.LittleEndian.PutUint64(b[40:48], uint64(s.Entries))
	return b
}
