package pg

import "flag"

type Config struct {
	Conn string `mapstructure:"conn"`
}

func (c *Config) RegisterFlags(flagPrefix string, f *flag.FlagSet) {
	f.StringVar(&c.Conn, flagPrefix+"pg.conn", "", `Postgres connection string`)
}
