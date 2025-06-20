package wazero_lmdb

type Option func(*module)

func WithCtxKeyMeta(key string) Option {
	return func(p *module) {
		p.ctxKeyMeta = key
	}
}

func WithCtxKeyTenantID(key string) Option {
	return func(p *module) {
		p.ctxKeyTenantID = key
	}
}

func WithCtxKeyLocalDir(key string) Option {
	return func(p *module) {
		p.ctxKeyLocalDir = key
	}
}

func WithCtxKeyBlockDir(key string) Option {
	return func(p *module) {
		p.ctxKeyLocalDir = key
	}
}
