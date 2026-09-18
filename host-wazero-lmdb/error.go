package wazero_lmdb

import (
	"os"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/tetratelabs/wazero/api"
)

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

var lmdbError = map[string]Errno{
	"MDB_KEYEXIST: Key/data pair already exists":                                    KeyExist,
	"MDB_NOTFOUND: No matching key/data pair found":                                 NotFound,
	"MDB_PAGE_NOTFOUND: Requested page not found":                                   PageNotFound,
	"MDB_CORRUPTED: Located page was wrong type":                                    Corrupted,
	"MDB_PANIC: Update of meta page failed or environment had fatal error":          Panic,
	"MDB_VERSION_MISMATCH: Database environment version mismatch":                   VersionMismatch,
	"MDB_INVALID: File is not an LMDB file":                                         Invalid,
	"MDB_MAP_FULL: Environment mapsize limit reached":                               MapFull,
	"MDB_DBS_FULL: Environment maxdbs limit reached":                                DBsFull,
	"MDB_READERS_FULL: Environment maxreaders limit reached":                        ReadersFull,
	"MDB_TLS_FULL: Thread-local storage keys full - too many environments open":     TLSFull,
	"MDB_TXN_FULL: Transaction has too many dirty pages - transaction too big":      TxnFull,
	"MDB_CURSOR_FULL: Internal error - cursor stack limit reached":                  CursorFull,
	"MDB_PAGE_FULL: Internal error - page has no more space":                        PageFull,
	"MDB_MAP_RESIZED: Database contents grew beyond environment mapsize":            MapResized,
	"MDB_INCOMPATIBLE: Operation and DB incompatible, or DB flags changed":          Incompatible,
	"MDB_BAD_RSLOT: Invalid reuse of reader locktable slot":                         BadRSlot,
	"MDB_BAD_TXN: Transaction must abort, has a child, or is invalid":               BadTxn,
	"MDB_BAD_VALSIZE: Unsupported size of key/DB name/data, or wrong DUPFIXED size": BadValSize,
	"MDB_BAD_DBI: The specified DBI handle was closed/changed unexpectedly":         BadDBI,
	os.ErrExist.Error():      Exist,
	os.ErrNotExist.Error():   NotExist,
	os.ErrPermission.Error(): Permission,
}

func writeError(m api.Module, meta *meta, err error) bool {
	if err == nil {
		writeUint32(m, meta.ptrErr, uint32(None))
		return false
	}
	str := err.Error()
	errno := Unknown
	if err, ok := err.(*lmdb.OpError); ok {
		str = err.Errno.Error()
		if n, ok := lmdbError[str]; ok {
			errno = n
		}
	}
	writeUint32(m, meta.ptrErr, uint32(errno))
	val := append(valBuf(m, meta), []byte(str)...)
	writeUint32(m, meta.ptrValLen, uint32(len(val)))
	return true
}
