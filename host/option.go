package wazero_lmdb

type Option func(*hostModule)

func WithCtxKeyMeta(key string) Option {
	return func(h *hostModule) {
		h.ctxKeyMeta = key
	}
}
func WithCtxKeyEnv(key string) Option {
	return func(h *hostModule) {
		h.ctxKeyEnv = key
	}
}
