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

type Client struct {
	httpClient *retryablehttp.Client
}

func NewClient(retryMax int, timeout time.Duration) *Client {
	c := retryablehttp.NewClient()
	c.RetryMax = retryMax
	c.HTTPClient.Timeout = timeout

	return &Client{
		httpClient: c,
	}
}

func (c *Client) GetAllDownloadFileInfo(ctx context.Context) ([]DownloadFileInfo, error) {
	resp, err := c.httpClient.Get(GetAllDownloadFileInfoUrl)
	defer resp.Body.Close()
	if err != nil {
		return nil, errors.Wrap(err, "get all download file info")
	}

	if err := util_http.EnsureSuccessStatusCode(resp); err != nil {
		return nil, errors.Wrap(err, "get all download file info")
	}

	allInfos := make([]DownloadFileInfo, 0)
	if err := json.NewDecoder(resp.Body).Decode(&allInfos); err != nil {
		return nil, errors.Wrap(err, "get all download file info")
	}

	return allInfos, nil
}
