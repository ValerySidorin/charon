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
starting_version: 0
polling_interval: 1s
ring:
  heartbeat_period: 1s
  heartbeat_timeout: 2s
  instance_id: downloader_1
  kvstore:
    store: consul
    consul:
      host: "127.0.0.1:8500"
      consistentreads: true
    prefix: "charon/"
kvstore:
  store: consul
  consul:
    host: "127.0.0.1:8500"
    consistentreads: true
  prefix: "charon/"
diffstore:
  store: minio
  minio:
    endpoint: "localhost:9000"
    minio_root_user: minioadmin
    minio_root_password: minioadmin
`

	cfg := downloader.Config{}
	err := yaml.Unmarshal([]byte(conf), &cfg)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	d, err := downloader.New(cfg, prometheus.NewPedanticRegistry(), log.NewLogfmtLogger(os.Stdout))
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	d.StartAsync(ctx)
	d.AwaitRunning(ctx)

	time.Sleep(5 * time.Second)

	cfg2 := cfg
	cfg2.DownloadersRing.InstanceID = "downloader_2"

	d2, err := downloader.New(cfg2, prometheus.NewPedanticRegistry(), log.NewLogfmtLogger(os.Stdout))
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	d2.StartAsync(ctx2)
	d2.AwaitRunning(ctx2)

	select {
	case <-time.After(15 * time.Second):
		cancel2()
	}

	select {
	case <-time.After(15 * time.Second):
		cancel()
	}
}
