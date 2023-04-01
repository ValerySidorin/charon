# Quick start
Start using Charon in production is not as easy, as one can imagine. The steps are listed below:
- Clone this repo
- Write your own import plugin
- Choose your deploy strategy
- Up and run Charon

## Writing plugin
First of all, it needs to be mentioned, Charon is not using traditional go plugin. It is just an interface, that you need to implement and inject. An XML file, that will be passed as an io.Reader can be too large for dynamically loaded plugin or as a RPC/gRPC stream for hashicorps' go-plugin. 
Here is a plugin interface:
```go
// Location: github.com/ValerySidorin/charon/pkg/processor/plugin

type Plugin interface {
	Exec(ctx context.Context, stream io.Reader) error // Execute import
	Filter(recs []*record.Record) []*record.Record // Filter files, that you want to import
	GetBatches(recs []*record.Record) []*batch.Batch // Get file batches, if you want to process them in some order
	UpgradeVersion(ctx context.Context, version int) error // Upgrade your GAR storage version
	GetVersion(ctx context.Context) int // Get your GAR storage current version
}
```
Your plugin can have a configuration, that will be presented as:
```go
// Location: github.com/ValerySidorin/charon/pkg/processor/plugin

type Config struct {
    // Inject your own plugin config here
	Mock mock.Config `yaml:"mock"`
}
```
Also, you need to build your plugin in a constructor:
```go
// Location: github.com/ValerySidorin/charon/pkg/processor/plugin

func New(cfg Config, log log.Logger) (Plugin, error) {
    // Inject and return your own plugin here
	plug := mock.NewPlugin(cfg.Mock, log)
	return plug, nil
}
```
An example could be found at `github.com/ValerySidorin/charon/pkg/processor/plugin/mock`

## Modes
By default charon starts all it's components in single copy in one binary. It means, one downloader and one processor will run in a single process. It can be defined explicitly:
```sh
charon -target all
```
But Charon is a highly scalable software, so it can also be deployed as a microservice. Here is the way, how to define, what component to use:
```sh
charon -target downloader
```
With that being said, you can combine any amount of charon components in multiple processes or even multiple hosts. Available components are:
- all
- downloader
- processor

## Deploy
The best way to run Charon is in Docker container. You can also run Charon as a binary on your machine, but please do not forget to configure all it's components instance ID via configuration or cli arguments, in case you are trying to replicate it. If not provided, hostname will be chosen as and instance ID for all components (which is not a problem if you are trying to run charon in container, because in that case hostname is a container ID). If IDs intersect between some instances, Charon will automatically consider its a single instance. You must avoid it.
```yaml
downloader:
  ring:
    instance_id: downloader-1
processor:
  ring:
    instance_id: processor-1
```
```sh
charon -downloader.ring.instance-id downloader-1 -processor.ring.instance-id processor-1
```
### Examples
#### Local
```sh
charon -target downloader -config.file C:/charon/cfg.yaml -downloader.ring.instance-id downloader-1 -downloader.temp-dir C:/charon/downloader -downloader.polling-interval 10s 
```
```sh
charon -target processor -config.file C:/charon/cfg.yaml -processor.ring.instance-id processor-1 -processor.plugin.mock.type mock
```
#### Docker
```sh
docker compose up -d
```
All example configs for Docker deployment can be found at development/