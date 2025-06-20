package lmdb

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
