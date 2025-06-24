package wazero_lmdb

type Option func(*module)

func WithCtxKeyMeta(key string) Option {
	return func(p *module) {
		p.ctxKeyMeta = key
	}
}
func WithCtxKeyEnv(key string) Option {
	return func(p *module) {
		p.ctxKeyEnv = key
	}
}
