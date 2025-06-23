package wazero_lmdb

type Option func(*module)

func WithCtxKeyMeta(key string) Option {
	return func(p *module) {
		p.ctxKeyMeta = key
	}
}

func WithCtxKeyPath(key string) Option {
	return func(p *module) {
		p.ctxKeyPath = key
	}
}

func WithCtxKeyMaxDBs(key string) Option {
	return func(p *module) {
		p.ctxKeyMaxDBs = key
	}
}

func WithCtxKeyMapSize(key string) Option {
	return func(p *module) {
		p.ctxKeyMapSize = key
	}
}
