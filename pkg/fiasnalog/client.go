package fiasnalog

import (
	"context"
	"encoding/json"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/pkg/errors"

	util_http "github.com/ValerySidorin/charon/pkg/util/http"
)

const (
	GetAllDownloadFileInfoUrl  = "https://fias.nalog.ru/WebServices/Public/GetAllDownloadFileInfo"
	GetLastDownloadFileInfoUrl = "https://fias.nalog.ru/WebServices/Public/GetLastDownloadFileInfo"
)

type Config struct {
	Timeout  int64 `yaml:"timeout"`
	RetryMax int   `yaml:"retry_max"`
}

type Client struct {
	httpClient *retryablehttp.Client
}

func NewClient(cfg Config) *Client {
	c := retryablehttp.NewClient()
	c.RetryMax = cfg.RetryMax
	c.HTTPClient.Timeout = time.Duration(cfg.Timeout) * time.Second

	return &Client{
		httpClient: c,
	}
}

func (c *Client) GetAllDownloadFileInfo(ctx context.Context) (*[]DownloadFileInfo, error) {
	req, err := retryablehttp.NewRequest("get", GetAllDownloadFileInfoUrl, nil)
	if err != nil {
		return nil, errors.Wrap(err, "get all download file info")
	}

	resp, err := c.httpClient.Do(req.WithContext(ctx))
	defer resp.Body.Close()
	if err != nil {
		return nil, errors.Wrap(err, "get all download file info")
	}

	if err := util_http.EnsureSuccessStatusCode(resp); err != nil {
		return nil, errors.Wrap(err, "get all download file info")
	}

	res := make([]DownloadFileInfo, 0)
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, errors.Wrap(err, "get all download file info")
	}

	return &res, nil
}

func (c *Client) GetLastDownloadFileInfo(ctx context.Context) (*DownloadFileInfo, error) {
	req, err := retryablehttp.NewRequest("get", GetLastDownloadFileInfoUrl, nil)
	if err != nil {
		return nil, errors.Wrap(err, "get last download file info")
	}

	resp, err := c.httpClient.Do(req.WithContext(ctx))
	defer resp.Body.Close()
	if err != nil {
		return nil, errors.Wrap(err, "get all download file info")
	}

	if err := util_http.EnsureSuccessStatusCode(resp); err != nil {
		return nil, errors.Wrap(err, "get all download file info")
	}

	res := DownloadFileInfo{}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, errors.Wrap(err, "get all download file info")
	}

	return &res, nil
}
