package queue

import (
	"time"

	"github.com/nats-io/nats.go"
)

const (
	defaultPublishAckTimeout = 2 * time.Second
)

type queueConfig struct {
	publishAckTimeout time.Duration
	natsOptions       []nats.Option
	logger            Logger
	enableDebugLog    bool
}

// Config 定义消息队列初始化配置。
// 适合通过 yaml/json/mapstructure 反序列化后直接用于创建消息队列实例。
type Config struct {
	URL string `json:"url" yaml:"url" mapstructure:"url"`
	// PublishAckTimeout 控制 Publish 场景 ACK 等待超时，<=0 时使用默认值。
	PublishAckTimeout time.Duration `json:"publishAckTimeout" yaml:"publishAckTimeout" mapstructure:"publishAckTimeout"`
	// EnableDebugLog 控制是否打印 debug 日志。
	EnableDebugLog bool `json:"enableDebugLog" yaml:"enableDebugLog" mapstructure:"enableDebugLog"`
	// Logger 与 NatsOptions 不建议做配置文件反序列化，通常由代码注入。
	Logger      Logger        `json:"-" yaml:"-" mapstructure:"-"`
	NatsOptions []nats.Option `json:"-" yaml:"-" mapstructure:"-"`
}

func defaultQueueConfig() queueConfig {
	return queueConfig{
		publishAckTimeout: defaultPublishAckTimeout,
		logger:            defaultLogger(),
		enableDebugLog:    false,
	}
}

type QueueOption func(*queueConfig)

func WithPublishAckTimeout(timeout time.Duration) QueueOption {
	return func(cfg *queueConfig) {
		if timeout > 0 {
			cfg.publishAckTimeout = timeout
		}
	}
}

func WithNatsOptions(natsOptions ...nats.Option) QueueOption {
	return func(cfg *queueConfig) {
		if len(natsOptions) > 0 {
			cfg.natsOptions = natsOptions
		}
	}
}

// WithLogger 配置外部注入日志器。
func WithLogger(logger Logger) QueueOption {
	return func(cfg *queueConfig) {
		if logger != nil {
			cfg.logger = logger
		}
	}
}

// WithDebugLogEnabled 控制是否输出 Debug 级别日志。
func WithDebugLogEnabled(enabled bool) QueueOption {
	return func(cfg *queueConfig) {
		cfg.enableDebugLog = enabled
	}
}

func applyQueueOptions(options []QueueOption) queueConfig {
	cfg := defaultQueueConfig()
	for _, option := range options {
		if option != nil {
			option(&cfg)
		}
	}
	return cfg
}

func queueOptionsFromConfig(cfg Config) []QueueOption {
	options := make([]QueueOption, 0, 4)
	if cfg.PublishAckTimeout > 0 {
		options = append(options, WithPublishAckTimeout(cfg.PublishAckTimeout))
	}
	options = append(options, WithDebugLogEnabled(cfg.EnableDebugLog))
	if cfg.Logger != nil {
		options = append(options, WithLogger(cfg.Logger))
	}
	if len(cfg.NatsOptions) > 0 {
		options = append(options, WithNatsOptions(cfg.NatsOptions...))
	}
	return options
}
