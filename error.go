package wazero_lmdb

var errors = map[string]string{
	"MDB_KEYEXIST":         "Key/data pair already exists",
	"MDB_NOTFOUND":         "No matching key/data pair found",
	"MDB_PAGE_NOTFOUND":    "Requested page not found",
	"MDB_CORRUPTED":        "Located page was wrong type",
	"MDB_PANIC":            "Update of meta page failed or environment had fatal error",
	"MDB_VERSION_MISMATCH": "Database environment version mismatch",
	"MDB_INVALID":          "File is not an LMDB file",
	"MDB_MAP_FULL":         "Environment mapsize limit reached",
	"MDB_DBS_FULL":         "Environment maxdbs limit reached",
	"MDB_READERS_FULL":     "Environment maxreaders limit reached",
	"MDB_TLS_FULL":         "Thread-local storage keys full - too many environments open",
	"MDB_TXN_FULL":         "Transaction has too many dirty pages - transaction too big",
	"MDB_CURSOR_FULL":      "Internal error - cursor stack limit reached",
	"MDB_PAGE_FULL":        "Internal error - page has no more space",
	"MDB_MAP_RESIZED":      "Database contents grew beyond environment mapsize",
	"MDB_INCOMPATIBLE":     "Operation and DB incompatible, or DB flags changed",
	"MDB_BAD_RSLOT":        "Invalid reuse of reader locktable slot",
	"MDB_BAD_TXN":          "Transaction must abort, has a child, or is invalid",
	"MDB_BAD_VALSIZE":      "Unsupported size of key/DB name/data, or wrong DUPFIXED size",
	"MDB_BAD_DBI":          "The specified DBI handle was closed/changed unexpectedly",
}
