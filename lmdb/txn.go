package lmdb

type Txn struct {
	envID uint32
	id    uint32
}

func (t *Txn) DbCreate(name string, flags uint32) uint32 {
	txnID = t.id
	expFlags = flags
	setKey([]byte(name))
	lmdbDbCreate()
	return expDbi
}

func (t *Txn) Put(dbi uint32, key, val []byte) {
	txnID = t.id
	expDbi = dbi
	setKey(key)
	setVal(val)
	lmdbPut()
}

func (t *Txn) Commit() {
	txnID = t.id
	lmdbCommit()
}
