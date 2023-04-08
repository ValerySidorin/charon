package log

import (
	"flag"
	"fmt"
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/weaveworks/common/logging"
)

var (
	Logger = log.NewNopLogger()
)

type Config struct {
	LogFormat string            `mapstructure:"log_format"`
	LogLevel  string            `mapstructure:"log_level"`
	Log       logging.Interface `mapstructure:"-"`
}

func (c *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&c.LogFormat, "log.format", "logfmt", "Output log messages in the given format. Valid formats: [logfmt, json]")
	f.StringVar(&c.LogLevel, "log.level", "info", "Only log messages with the given severity or above. Valid levels: [debug, info, warn, error]")
}

// Log config with properties, that are used by logger.

func InitLogger(cfg *Config) {
	logFormat := logging.Format{}
	logLevel := logging.Level{}

	logFormat.Set(cfg.LogFormat)
	logLevel.Set(cfg.LogLevel)

	l := newBasicLogger(logLevel, logFormat)

	logger := log.With(l, "caller", log.Caller(5))
	Logger = level.NewFilter(logger, logLevel.Gokit)

	cfg.Log = logging.GoKit(level.NewFilter(log.With(l, "caller", log.Caller(6)), logLevel.Gokit))
}

func newBasicLogger(l logging.Level, format logging.Format) log.Logger {
	var logger log.Logger
	if format.String() == "json" {
		logger = log.NewJSONLogger(log.NewSyncWriter(os.Stderr))
	} else {
		logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	}

	return log.With(logger, "ts", log.DefaultTimestampUTC)
}

func NewDefaultLogger(l logging.Level, format logging.Format) log.Logger {
	logger := newBasicLogger(l, format)
	return level.NewFilter(log.With(logger, "ts", log.DefaultTimestampUTC), l.Gokit)
}

func CheckFatal(location string, err error) {
	if err != nil {
		logger := level.Error(Logger)
		if location != "" {
			logger = log.With(logger, "msg", "error "+location)
		}

		_ = logger.Log("err", fmt.Sprintf("%+v", err))
		os.Exit(1)
	}
}
