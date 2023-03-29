package config

import (
	"flag"

	"github.com/ValerySidorin/charon/pkg/wal/config/pg"
)

type Config struct {
	Name        string `yaml:"name"`
	Store       string `yaml:"store"`
	StoreConfig `yaml:",inline"`
}

type StoreConfig struct {
	Pg pg.Config `yaml:"pg"`
}

func (c *Config) RegisterFlags(flagPrefix string, f *flag.FlagSet) {
	c.Pg.RegisterFlags(flagPrefix, f)

	f.StringVar(&c.Name, flagPrefix+"name", "", `WAL name. Available for processor only.`)
	f.StringVar(&c.Store, flagPrefix+"store", "memberlist", `Store, that will be used to persist WAL.`)
}
