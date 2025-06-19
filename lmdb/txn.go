package lmdb

type Txn struct {
	envID uint32
	id    uint32
}

func (t *Txn) DbOpen(name string, flags uint32) uint32 {
	txnID = t.id
	expFlg = flags
	setKey([]byte(name))
	lmdbDbOpen()
	return expDbi
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

func (t *Txn) Commit() {
	txnID = t.id
	lmdbCommit()
}

func (t *Txn) Abort() {
	txnID = t.id
	lmdbAbort()
}
