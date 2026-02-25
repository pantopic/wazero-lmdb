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
// https://github.com/PowerDNS/lmdb-go/blob/v1.9.3/lmdb/cursor.go#L16
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

func BeginTxn(parent *Txn, flags uint32) (txn *Txn, err error) {
	if parent != nil {
		txnID = parent.id
	} else {
		txnID = 0
	}
	expFlg = flags
	lmdbBegin()
	if errCode > 0 {
		err = opError{Errno(errCode), getVal()}
		return
	}
	txn = &Txn{txnID}
	return
}

func View(fn func(*Txn) error) (err error) {
	txn, err := BeginTxn(nil, Readonly)
	if err != nil {
		return
	}
	err = fn(txn)
	txn.Abort()
	return
}

func Update(fn func(*Txn) error) (err error) {
	txn, err := BeginTxn(nil, 0)
	if err != nil {
		return
	}
	if err = fn(txn); err == nil {
		err = txn.Commit()
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

func (t *Txn) CreateDBI(name string, flags uint32) (dbi DBI, err error) {
	return t.OpenDBI(name, flags|Create)
}

func (t *Txn) OpenDBI(name string, flags uint32) (dbi DBI, err error) {
	txnID = t.id
	expFlg = flags
	setKey([]byte(name))
	lmdbDbOpen()
	if errCode > 0 {
		err = opError{Errno(errCode), getVal()}
		return
	}
	dbi = expDbi
	return
}

func (t *Txn) Drop(dbi DBI) (err error) {
	txnID = t.id
	expDbi = dbi
	lmdbDbDrop()
	if errCode > 0 {
		err = opError{Errno(errCode), getVal()}
	}
	return
}

func (t *Txn) Stat(dbi DBI) (s *Stat, err error) {
	txnID = t.id
	expDbi = dbi
	lmdbDbStat()
	if errCode > 0 {
		err = opError{Errno(errCode), getVal()}
		return
	}
	s = stat.from(getVal())
	return
}

func (t *Txn) Put(dbi DBI, k, v []byte, flags uint32) (err error) {
	txnID = t.id
	expDbi = dbi
	expFlg = flags
	setKey(k)
	setVal(v)
	lmdbPut()
	if errCode > 0 {
		err = opError{Errno(errCode), getVal()}
	}
	return
}

func (t *Txn) Get(dbi DBI, k []byte) (v []byte, err error) {
	txnID = t.id
	expDbi = dbi
	setKey(k)
	lmdbGet()
	if errCode > 0 {
		err = opError{Errno(errCode), getVal()}
		return
	}
	v = append(v, getVal()...)
	return
}

func (t *Txn) Del(dbi DBI, k, v []byte) (err error) {
	txnID = t.id
	expDbi = dbi
	setKey(k)
	setVal(v)
	lmdbDel()
	if errCode > 0 {
		err = opError{Errno(errCode), getVal()}
	}
	return
}

func (t *Txn) OpenCursor(dbi DBI) (cur *Cursor, err error) {
	txnID = t.id
	expDbi = dbi
	lmdbCursorOpen()
	if errCode > 0 {
		err = opError{Errno(errCode), getVal()}
		return
	}
	cur = &Cursor{curID}
	return
}

func (t *Txn) Commit() (err error) {
	txnID = t.id
	lmdbCommit()
	if errCode > 0 {
		err = opError{Errno(errCode), getVal()}
	}
	return
}

func (t *Txn) Abort() {
	txnID = t.id
	lmdbAbort()
}

func (t *Txn) Sub(fn func(*Txn) error) (err error) {
	txn, err := BeginTxn(t, 0)
	if err != nil {
		return
	}
	if err = fn(txn); err == nil {
		err = txn.Commit()
	} else {
		txn.Abort()
	}
	return
}

// Cursor represents an LMDB cursor
// See https://pkg.go.dev/github.com/PowerDNS/lmdb-go/lmdb#Cursor
type Cursor struct {
	id uint32
}

func (c *Cursor) Get(k, v []byte, flags uint32) (rk []byte, rv []byte, err error) {
	curID = c.id
	expFlg = flags
	setKey(k)
	setVal(v)
	lmdbCursorGet()
	if errCode > 0 {
		return nil, nil, opError{Errno(errCode), getVal()}
	}
	rk = append(rk, getKey()...)
	rv = append(rv, getVal()...)
	return
}

func (c *Cursor) Put(k, v []byte, flags uint32) (err error) {
	curID = c.id
	expFlg = flags
	setKey(k)
	setVal(v)
	lmdbCursorPut()
	if errCode > 0 {
		err = opError{Errno(errCode), getVal()}
	}
	return
}

func (c *Cursor) Del(flags uint32) (err error) {
	curID = c.id
	expFlg = flags
	lmdbCursorDel()
	if errCode > 0 {
		err = opError{Errno(errCode), getVal()}
	}
	return
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
