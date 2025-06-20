package lmdb

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
