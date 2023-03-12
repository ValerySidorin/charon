package main

import (
	"fmt"
	"os"

	"github.com/ValerySidorin/charon/pkg/downloader"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
	"gopkg.in/yaml.v2"
)

func main() {
	conf := `
starting_version: 0
polling_interval: 1s
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
	d.StartAsync(ctx)
	d.AwaitRunning(ctx)

	// cfg2 := cfg
	// cfg2.DownloadersRing.InstanceID = "downloader_2"

	// ctx2, _ := context.WithCancel(context.Background())

	// time.Sleep(10 * time.Second)

	// d2, err := downloader.New(ctx, cfg2, prometheus.NewPedanticRegistry(), log.NewLogfmtLogger(os.Stdout))
	// if err != nil {
	// 	fmt.Println(err.Error())
	// 	os.Exit(1)
	// }
	// d2.StartAsync(ctx2)
	// d2.AwaitRunning(ctx2)

	select {}
}
