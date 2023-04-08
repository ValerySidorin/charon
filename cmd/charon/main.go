package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/ValerySidorin/charon/pkg/charon"
	util_log "github.com/ValerySidorin/charon/pkg/util/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/viper"
)

const (
	configFileOption      = "config.file"
	configExpandEnvOption = "config.expand-env"
)

func main() {
	cfg := charon.Config{}

	conf := parseConfigFileParameter(os.Args[1:])

	cfg.RegisterFlags(flag.CommandLine, util_log.Logger)

	if err := LoadConfig(conf, &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error loading config from: %s: %v\n", conf, err)
		os.Exit(1)
	}

	flag.CommandLine.VisitAll(func(f *flag.Flag) {
		env := strings.ToUpper(f.Name)
		env = strings.Replace(env, ".", "_", -1)
		env = strings.Replace(env, "-", "_", -1)
		env = "CHARON_" + env

		val := os.Getenv(env)
		if val != "" {
			_ = flag.CommandLine.Lookup(f.Name).Value.Set(val)
		}
	})

	flagext.IgnoredFlag(flag.CommandLine, configFileOption, "Configuration file to load.")

	flag.CommandLine.Usage = func() {}
	flag.CommandLine.Init(flag.CommandLine.Name(), flag.ContinueOnError)

	if err := flag.CommandLine.Parse(os.Args[1:]); err != nil {
		fmt.Fprintln(flag.CommandLine.Output(), "Run with -help to get a list of available parameters")
	}

	util_log.InitLogger(&cfg.Log)

	c, err := charon.New(cfg, prometheus.DefaultRegisterer)
	util_log.CheckFatal("initializing application", err)

	_ = level.Info(util_log.Logger).Log("msg", "Starting application")

	err = c.Run()

	util_log.CheckFatal("running application", err)
}

func parseConfigFileParameter(args []string) (configFile string) {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	fs.StringVar(&configFile, configFileOption, "", "")

	for len(args) > 0 {
		_ = fs.Parse(args)
		args = args[1:]
	}

	return
}

func LoadConfig(filename string, cfg *charon.Config) error {
	v := viper.New()

	if filename == "" {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		v.AddConfigPath("conf/")
	} else {
		var base = path.Base(filename)
		var ext = path.Ext(base)
		var name = strings.TrimSuffix(base, ext)
		v.SetConfigName(name)
		v.SetConfigType(ext[1:])
		v.AddConfigPath(strings.TrimSuffix(filename, base))
	}

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			fmt.Fprintln(os.Stdout, "Config file not found, only env and flag values will be presented.")
			return nil
		} else {
			return err
		}
	}
	if err := v.Unmarshal(&cfg); err != nil {
		return err
	}

	return nil
}
