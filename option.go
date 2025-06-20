package plugin_lmdb

type Option func(*plugin)

func WithCtxKeyMeta(key string) Option {
	return func(p *plugin) {
		p.ctxKeyMeta = key
	}
}

func WithCtxKeyTenantID(key string) Option {
	return func(p *plugin) {
		p.ctxKeyTenantID = key
	}
}

func WithCtxKeyLocalDir(key string) Option {
	return func(p *plugin) {
		p.ctxKeyLocalDir = key
	}
}

func WithCtxKeyBlockDir(key string) Option {
	return func(p *plugin) {
		p.ctxKeyLocalDir = key
	}
}
