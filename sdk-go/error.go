package lmdb

const (
	None Errno = iota
	KeyExist
	NotFound
	PageNotFound
	Corrupted
	Panic
	VersionMismatch
	Invalid
	MapFull
	DBsFull
	ReadersFull
	TLSFull
	TxnFull
	CursorFull
	PageFull
	MapResized
	Incompatible
	BadRSlot
	BadTxn
	BadValSize
	BadDBI
	Exist
	NotExist
	Permission
	Unknown
)

type Errno uint32

func (err Errno) Error() string {
	return string(getVal())
}

type opError struct {
	code Errno
	msg  []byte
}

func (err opError) Error() string {
	return string(getVal())
}

func IsNotExist(err error) bool {
	return IsErrNo(err, NotExist)
}

func IsNotFound(err error) bool {
	return IsErrNo(err, NotFound)
}

func IsErrNo(err error, code Errno) bool {
	if err == nil {
		return false
	}
	if err, ok := err.(Errno); ok {
		return err == code
	}
	return false
}
