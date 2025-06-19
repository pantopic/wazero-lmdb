package lmdb

//export lmdbEnvOpen
func lmdbEnvOpen()

//export lmdbEnvStat
func lmdbEnvStat()

//export lmdbEnvClose
func lmdbEnvClose()

//export lmdbEnvDelete
func lmdbEnvDelete()

type Environment struct {
	id uint32
}

func Open(name string) *Environment {
	setKey([]byte(name))
	lmdbEnvOpen()
	return &Environment{
		id: envID,
	}
}

func (e *Environment) Stat() []byte {
	envID = e.id
	lmdbEnvStat()
	return getVal()
}
