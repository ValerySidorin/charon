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
	LogFormat logging.Format    `yaml:"log_format"`
	LogLevel  logging.Level     `yaml:"log_level"`
	Log       logging.Interface `yaml:"-"`
}

func (c *Config) RegisterFlags(f *flag.FlagSet) {
	c.LogFormat.RegisterFlags(f)
	c.LogLevel.RegisterFlags(f)
}

func InitLogger(cfg *Config) {
	l := newBasicLogger(cfg.LogLevel, cfg.LogFormat)

	logger := log.With(l, "caller", log.Caller(5))
	Logger = level.NewFilter(logger, cfg.LogLevel.Gokit)

	cfg.Log = logging.GoKit(level.NewFilter(log.With(l, "caller", log.Caller(6)), cfg.LogLevel.Gokit))
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
