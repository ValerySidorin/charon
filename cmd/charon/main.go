package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/ValerySidorin/charon/pkg/charon"
	util_log "github.com/ValerySidorin/charon/pkg/util/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v3"
)

const (
	configFileOption      = "config.file"
	configExpandEnvOption = "config.expand-env"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stdout, "Charon is a horizontally scalable, fault tolerant, plugin-driven GAR (prev. FIAS) data processor.")
		os.Exit(0)
	}

	var cfg charon.Config

	conf, expandEnv := parseConfigFileParameter(os.Args[1:])

	cfg.RegisterFlags(flag.CommandLine, util_log.Logger)

	if err := LoadConfig(conf, expandEnv, &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error loading config from: %s: %v\n", conf, err)
		os.Exit(1)
	}

	flagext.IgnoredFlag(flag.CommandLine, configFileOption, "Configuration file to load.")
	_ = flag.CommandLine.Bool(configExpandEnvOption, false, "Expands ${var} or $var in config according to the values of the environment variables.")

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

func parseConfigFileParameter(args []string) (configFile string, configExpandEnv bool) {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	fs.StringVar(&configFile, configFileOption, "", "")
	fs.BoolVar(&configExpandEnv, configExpandEnvOption, false, "")

	for len(args) > 0 {
		_ = fs.Parse(args)
		args = args[1:]
	}

	return
}

func LoadConfig(filename string, expandEnv bool, cfg *charon.Config) error {
	buf, err := os.ReadFile(filename)
	if err != nil {
		return errors.Wrap(err, "Error reading config file")
	}

	if expandEnv {
		buf = expandEnvironmentVariables(buf)
	}

	dec := yaml.NewDecoder(bytes.NewReader(buf))
	dec.KnownFields(true)

	if err := dec.Decode(cfg); err != nil {
		return errors.Wrap(err, "Error parsing config file")
	}

	return nil
}

func expandEnvironmentVariables(config []byte) []byte {
	return []byte(os.Expand(string(config), func(key string) string {
		keyAndDefault := strings.SplitN(key, ":", 2)
		key = keyAndDefault[0]

		v := os.Getenv(key)
		if v == "" && len(keyAndDefault) == 2 {
			v = keyAndDefault[1]
		}
		return v
	}))
}
