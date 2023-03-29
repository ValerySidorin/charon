package pg

import "flag"

type Config struct {
	Conn string `yaml:"conn"`
}

func (c *Config) RegisterFlags(flagPrefix string, f *flag.FlagSet) {
	f.StringVar(&c.Conn, flagPrefix+"pg.conn", "", `Postgres connection string`)
}
