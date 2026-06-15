package actor

type Option func(*spawnConfig)

type spawnConfig struct {
	name        string
	mailboxSize int
	initArgs    []any
}

func WithName(name string) Option {
	return func(cfg *spawnConfig) {
		cfg.name = name
	}
}

func WithMailboxSize(size int) Option {
	return func(cfg *spawnConfig) {
		if size > 0 {
			cfg.mailboxSize = size
		}
	}
}

func WithInitArgs(args ...any) Option {
	return func(cfg *spawnConfig) {
		if len(args) == 0 {
			cfg.initArgs = nil
			return
		}
		cfg.initArgs = append([]any(nil), args...)
	}
}

func applyOptions(options ...Option) *spawnConfig {
	cfg := &spawnConfig{
		mailboxSize: 1024,
	}
	for _, opt := range options {
		opt(cfg)
	}
	return cfg
}
