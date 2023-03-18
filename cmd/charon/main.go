package main

import (
	"fmt"
	"os"
	"time"

	"github.com/ValerySidorin/charon/pkg/downloader"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
	"gopkg.in/yaml.v2"
)

func main() {
	conf := `
start_from:
  file_type: diff
  version: 20230317
polling_interval: 10s
local_dir: D://charon/downloader
fias_nalog: 
  timeout: 30s
  retry_max: 5
file_fetcher:
  buffer_size: 4096
ring:
  heartbeat_period: 1s
  heartbeat_timeout: 2s
  instance_id: downloader_1
  kvstore:
    store: consul
    consul:
      host: "127.0.0.1:8500"
      consistentreads: true
      acl_token: charonconsul
    prefix: "charon/"
diffstore:
  store: minio
  minio:
    endpoint: "127.0.0.1:9000"
    minio_root_user: charonminio
    minio_root_password: charonminio
walstore:
  store: pg
  pg:
    conn: postgres://charonpostgres:charonpostgres@localhost:5432/downloader
notifier:
  check_interval: 10s
  queue:
    type: nats
    conn: nats://charonnats:charonnats@localhost:4222
`

	cfg := downloader.Config{}
	err := yaml.Unmarshal([]byte(conf), &cfg)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	ctx, _ := context.WithCancel(context.Background())

	d, err := downloader.New(ctx, cfg, prometheus.NewPedanticRegistry(), log.NewLogfmtLogger(os.Stdout))
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	cfg2 := cfg
	cfg2.DownloadersRing.InstanceID = "downloader_2"
	cfg2.StartFrom.Version = 20230314

	ctx2, _ := context.WithCancel(context.Background())

	d2, err := downloader.New(ctx2, cfg2, prometheus.NewPedanticRegistry(), log.NewLogfmtLogger(os.Stdout))
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	d.StartAsync(ctx)
	d.AwaitRunning(ctx)

	time.Sleep(5 * time.Second) // For testing purposes, not required
	d2.StartAsync(ctx2)
	d2.AwaitRunning(ctx)

	fmt.Println(d.State().String())
	fmt.Println(d2.State().String())

	select {}
}
