package util

// This is just a config mapping, that we need to implement to be able to use viper.

import (
	"flag"
	"strings"
	"time"

	dstls "github.com/grafana/dskit/crypto/tls"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/kv/etcd"
	"github.com/grafana/dskit/kv/memberlist"
)

const (
	longPollDuration = 10 * time.Second
)

type CommonKVConfig struct {
	Store               string `mapstructure:"store"`
	Prefix              string `mapstructure:"prefix"`
	CommonKVStoreConfig `mapstructure:",squash"`
}

type CommonKVStoreConfig struct {
	Consul ConsulConfig `mapstructure:"consul"`
	Etcd   EtcdConfig   `mapstructure:"etcd"`

	MemberlistKV func() (*memberlist.KV, error) `mapstructure:"-"`
}

func (cfg *CommonKVConfig) RegisterFlagsWithPrefix(flagsPrefix, defaultPrefix string, f *flag.FlagSet) {
	// We need Consul flags to not have the ring prefix to maintain compatibility.
	// This needs to be fixed in the future (1.0 release maybe?) when we normalize flags.
	// At the moment we have consul.<flag-name>, and ring.store, going forward it would
	// be easier to have everything under ring, so ring.consul.<flag-name>
	cfg.Consul.RegisterFlags(f, flagsPrefix)
	cfg.Etcd.RegisterFlagsWithPrefix(f, flagsPrefix)

	if flagsPrefix == "" {
		flagsPrefix = "ring."
	}

	// Allow clients to override default store by setting it before calling this method.
	if cfg.Store == "" {
		cfg.Store = "consul"
	}

	f.StringVar(&cfg.Prefix, flagsPrefix+"prefix", defaultPrefix, "The prefix for the keys in the store. Should end with a /.")
	f.StringVar(&cfg.Store, flagsPrefix+"store", cfg.Store, "Backend storage to use for the ring. Supported values are: consul, etcd, inmemory, memberlist, multi.")
}

// Consul config
type ConsulConfig struct {
	Host              string        `mapstructure:"host"`
	ACLToken          string        `mapstructure:"acl_token" category:"advanced"`
	HTTPClientTimeout time.Duration `mapstructure:"http_client_timeout" category:"advanced"`
	ConsistentReads   bool          `mapstructure:"consistent_reads" category:"advanced"`
	WatchKeyRateLimit float64       `mapstructure:"watch_rate_limit" category:"advanced"`
	WatchKeyBurstSize int           `mapstructure:"watch_burst_size" category:"advanced"`
	CasRetryDelay     time.Duration `mapstructure:"cas_retry_delay" category:"advanced"`
}

func (cfg *ConsulConfig) RegisterFlags(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Host, prefix+"consul.hostname", "localhost:8500", "Hostname and port of Consul.")
	f.StringVar(&cfg.ACLToken, prefix+"consul.acl-token", "", "ACL Token used to interact with Consul.")
	f.DurationVar(&cfg.HTTPClientTimeout, prefix+"consul.client-timeout", 2*longPollDuration, "HTTP timeout when talking to Consul")
	f.BoolVar(&cfg.ConsistentReads, prefix+"consul.consistent-reads", false, "Enable consistent reads to Consul.")
	f.Float64Var(&cfg.WatchKeyRateLimit, prefix+"consul.watch-rate-limit", 1, "Rate limit when watching key or prefix in Consul, in requests per second. 0 disables the rate limit.")
	f.IntVar(&cfg.WatchKeyBurstSize, prefix+"consul.watch-burst-size", 1, "Burst size used in rate limit. Values less than 1 are treated as 1.")
	f.DurationVar(&cfg.CasRetryDelay, prefix+"consul.cas-retry-delay", 1*time.Second, "Maximum duration to wait before retrying a Compare And Swap (CAS) operation.")
}

// Etcd configs
type EtcdConfig struct {
	Endpoints   []string      `mapstructure:"endpoints"`
	DialTimeout time.Duration `mapstructure:"dial_timeout" category:"advanced"`
	MaxRetries  int           `mapstructure:"max_retries" category:"advanced"`
	EnableTLS   bool          `mapstructure:"tls_enabled" category:"advanced"`
	TLS         ClientConfig  `mapstructure:",inline"`

	UserName string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

type ClientConfig struct {
	CertPath           string `mapstructure:"tls_cert_path" category:"advanced"`
	KeyPath            string `mapstructure:"tls_key_path" category:"advanced"`
	CAPath             string `mapstructure:"tls_ca_path" category:"advanced"`
	ServerName         string `mapstructure:"tls_server_name" category:"advanced"`
	InsecureSkipVerify bool   `mapstructure:"tls_insecure_skip_verify" category:"advanced"`
	CipherSuites       string `mapstructure:"tls_cipher_suites" category:"advanced" doc:"description_method=GetTLSCipherSuitesLongDescription"`
	MinVersion         string `mapstructure:"tls_min_version" category:"advanced"`
}

func (cfg *EtcdConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	cfg.Endpoints = []string{}
	f.Var((*flagext.StringSlice)(&cfg.Endpoints), prefix+"etcd.endpoints", "The etcd endpoints to connect to.")
	f.DurationVar(&cfg.DialTimeout, prefix+"etcd.dial-timeout", 10*time.Second, "The dial timeout for the etcd connection.")
	f.IntVar(&cfg.MaxRetries, prefix+"etcd.max-retries", 10, "The maximum number of retries to do for failed ops.")
	f.BoolVar(&cfg.EnableTLS, prefix+"etcd.tls-enabled", false, "Enable TLS.")
	f.StringVar(&cfg.UserName, prefix+"etcd.username", "", "Etcd username.")
	f.StringVar(&cfg.Password, prefix+"etcd.password", "", "Etcd password.")
	cfg.TLS.RegisterFlagsWithPrefix(prefix+"etcd", f)
}

func (cfg *ClientConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	prefix = strings.TrimRight(prefix, ".")

	f.StringVar(&cfg.CertPath, prefix+".tls-cert-path", "", "Path to the client certificate file, which will be used for authenticating with the server. Also requires the key path to be configured.")
	f.StringVar(&cfg.KeyPath, prefix+".tls-key-path", "", "Path to the key file for the client certificate. Also requires the client certificate to be configured.")
	f.StringVar(&cfg.CAPath, prefix+".tls-ca-path", "", "Path to the CA certificates file to validate server certificate against. If not set, the host's root CA certificates are used.")
	f.StringVar(&cfg.ServerName, prefix+".tls-server-name", "", "Override the expected name on the server certificate.")
	f.BoolVar(&cfg.InsecureSkipVerify, prefix+".tls-insecure-skip-verify", false, "Skip validating server certificate.")
	f.StringVar(&cfg.CipherSuites, prefix+".tls-cipher-suites", "", "Override the default cipher suite list (separated by commas).")
	f.StringVar(&cfg.MinVersion, prefix+".tls-min-version", "", "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13")
}

func (c *CommonKVConfig) ToKVConfig() kv.Config {
	kc := kv.Config{}

	kc.Store = c.Store
	kc.Prefix = c.Prefix

	if c.Store == "consul" {
		kc.Consul = consul.Config{}

		kc.Consul.Host = c.Consul.Host
		kc.Consul.ACLToken = flagext.SecretWithValue(c.Consul.ACLToken)
		kc.Consul.HTTPClientTimeout = c.Consul.HTTPClientTimeout
		kc.Consul.ConsistentReads = c.Consul.ConsistentReads
		kc.Consul.WatchKeyRateLimit = c.Consul.WatchKeyRateLimit
		kc.Consul.WatchKeyBurstSize = c.Consul.WatchKeyBurstSize
		kc.Consul.CasRetryDelay = c.Consul.CasRetryDelay
	}

	if c.Store == "etcd" {
		kc.Etcd = etcd.Config{}
		kc.Etcd.TLS = dstls.ClientConfig{}

		kc.Etcd.Endpoints = c.Etcd.Endpoints
		kc.Etcd.DialTimeout = c.Etcd.DialTimeout
		kc.Etcd.MaxRetries = c.Etcd.MaxRetries
		kc.Etcd.EnableTLS = c.Etcd.EnableTLS
		kc.Etcd.TLS.CertPath = c.Etcd.TLS.CertPath
		kc.Etcd.TLS.KeyPath = c.Etcd.TLS.KeyPath
		kc.Etcd.TLS.CAPath = c.Etcd.TLS.CAPath
		kc.Etcd.TLS.ServerName = c.Etcd.TLS.ServerName
		kc.Etcd.TLS.InsecureSkipVerify = c.Etcd.TLS.InsecureSkipVerify
		kc.Etcd.TLS.CipherSuites = c.Etcd.TLS.CipherSuites
		kc.Etcd.TLS.MinVersion = c.Etcd.TLS.MinVersion
		kc.Etcd.UserName = c.Etcd.UserName
		kc.Etcd.Password = flagext.SecretWithValue(c.Etcd.Password)
	}

	if c.Store == "memberlist" {
		kc.MemberlistKV = c.MemberlistKV
	}

	return kc
}
