package main

import (
	"fmt"
	"os"

	"github.com/ValerySidorin/charon/pkg/downloader"
	"github.com/ValerySidorin/charon/pkg/processor"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
	"gopkg.in/yaml.v2"
)

func main() {
	testProcessors()
	testDownloaders()

	select {}
}

func testDownloaders() {
	conf := `
start_from:
  file_type: diff
  version: 20230314
polling_interval: 10s
temp_dir: D://charon/downloader
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
      host: "localhost:8500"
      consistentreads: true
      acl_token: charon
    prefix: "charon/"
diff_store:
  store: minio
  minio:
    endpoint: "localhost:9000"
    minio_root_user: charon
    minio_root_password: charonpwd
wal:
  store: pg
  pg:
    conn: postgres://charon:charonpwd@localhost:5432/charon
notifier:
  check_interval: 10s
  queue:
    type: nats
    conn: nats://charon:charonpwd@localhost:4222
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

	ctx2, _ := context.WithCancel(context.Background())

	d2, err := downloader.New(ctx2, cfg2, prometheus.NewPedanticRegistry(), log.NewLogfmtLogger(os.Stdout))
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	d.StartAsync(ctx)
	d2.StartAsync(ctx2)

	d.AwaitRunning(ctx)
	d2.AwaitRunning(ctx)

	fmt.Println(d.State().String())
	fmt.Println(d2.State().String())
}

func testProcessors() {
	conf := `
worker_pool: 2
msg_buffer: 100
ring:
  key: mock_processor
  heartbeat_period: 1s
  heartbeat_timeout: 2s
  instance_id: processor_1
  kvstore:
    store: consul 
    consul:
      host: "localhost:8500"
      consistentreads: true
      acl_token: charon
    prefix: "charon/"
diff_store:
  store: minio
  minio:
    endpoint: "localhost:9000"
    minio_root_user: charon
    minio_root_password: charonpwd
wal:
  store: pg
  pg:
    conn: postgres://charon:charonpwd@localhost:5432/charon
queue:
  type: nats
  conn: nats://charon:charonpwd@localhost:4222
`

	cfg := processor.Config{}
	err := yaml.Unmarshal([]byte(conf), &cfg)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	ctx := context.Background()
	p, err := processor.New(ctx, cfg, prometheus.NewPedanticRegistry(), log.NewLogfmtLogger(os.Stdout))
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	p.StartAsync(ctx)
	p.AwaitRunning(ctx)
}
