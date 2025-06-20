package lmdb

// Host Module flags
const (
	// Flag for EnvOpen indicating selection of block storage directory rather than local storage (default).
	Block uint32 = 1 << 31
)

// LMDB multiuse flags
// See https://pkg.go.dev/github.com/PowerDNS/lmdb-go/lmdb#pkg-constants
const (
	DupSort     uint32 = 0x00004
	Current     uint32 = 0x00040
	Readonly    uint32 = 0x20000
	Create      uint32 = 0x40000
	NoReadahead uint32 = 0x800000
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

// Env represents an LMDB environment (database file)
// See https://pkg.go.dev/github.com/PowerDNS/lmdb-go/lmdb#Env
type Env struct {
	id uint32
}

func Open(name string, flags uint32) *Env {
	setKey([]byte(name))
	expFlg = flags
	lmdbEnvOpen()
	return &Env{envID}
}

func (e *Env) Stat() []byte {
	envID = e.id
	lmdbEnvStat()
	return getVal()
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

func (t *Txn) DbOpen(name string, flags uint32) uint32 {
	txnID = t.id
	expFlg = flags
	setKey([]byte(name))
	lmdbDbOpen()
	return expDbi
}

func (t *Txn) DbDrop(dbi uint32) {
	txnID = t.id
	expDbi = dbi
	lmdbDbDrop()
}

func (t *Txn) Stat(dbi uint32) []byte {
	txnID = t.id
	expDbi = dbi
	lmdbDbStat()
	return getVal()
}

func (t *Txn) Put(dbi uint32, key, val []byte) {
	txnID = t.id
	expDbi = dbi
	setKey(key)
	setVal(val)
	lmdbPut()
}

func (t *Txn) Get(dbi uint32, key []byte) []byte {
	txnID = t.id
	expDbi = dbi
	setKey(key)
	lmdbGet()
	return getVal()
}

func (t *Txn) Del(dbi uint32, key, val []byte) {
	txnID = t.id
	expDbi = dbi
	setKey(key)
	setVal(val)
	lmdbDel()
}

func (t *Txn) CursorOpen(dbi uint32) *Cursor {
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
