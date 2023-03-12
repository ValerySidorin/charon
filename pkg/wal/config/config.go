package config

import "github.com/ValerySidorin/charon/pkg/wal/config/pg"

type Config struct {
	Store       string `yaml:"store"`
	StoreConfig `yaml:",inline"`
}

type StoreConfig struct {
	Pg pg.Config `yaml:"pg"`
}
