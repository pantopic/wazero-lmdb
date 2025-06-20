package lmdb

// Plugin flags
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
