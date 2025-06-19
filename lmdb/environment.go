package lmdb

type Env struct {
	id uint32
}

func Open(name string) *Env {
	setKey([]byte(name))
	lmdbEnvOpen()
	return &Env{
		id: envID,
	}
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

func (e *Env) BeginTxn(parent *Txn, readonly bool) *Txn {
	envID = e.id
	if parent != nil {
		txnID = parent.id
	} else {
		txnID = 0
	}
	lmdbBegin()
	return &Txn{
		envID: e.id,
		id:    txnID,
	}
}
