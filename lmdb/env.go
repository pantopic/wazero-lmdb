package lmdb

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
