package filefetcher

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cavaliergopher/grab/v3"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
)

const (
	FileName = "delta.zip"
)

type Config struct {
	BufferSize int `yaml:"buffer_size"`
}

type FileFetcher struct {
	grabClient *grab.Client
	cfg        Config
	log        log.Logger
}

func NewClient(cfg Config, log log.Logger) *FileFetcher {
	c := grab.NewClient()
	c.BufferSize = cfg.BufferSize

	return &FileFetcher{
		grabClient: c,
		cfg:        cfg,
		log:        log,
	}
}

func (f *FileFetcher) Download(path string, version int, url string) error {
	versionStr := strconv.Itoa(version)
	if _, err := os.Stat(filepath.Join(path, versionStr)); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return errors.Wrap(err, "file fether os.Stat")
		}
	} else {
		entries, err := os.ReadDir(path)
		if err != nil {
			return errors.Wrap(err, "file fetcher os.ReadDir")
		}

		versionedDir := entries[0]
		if !strings.HasSuffix(versionedDir.Name(), versionStr) {
			os.RemoveAll(path)
			os.MkdirAll(path, os.ModeAppend)
		}
	}

	level.Info(f.log).Log("msg", fmt.Sprintf("start downloading file: %s", url))
	req, err := grab.NewRequest(filepath.Join(path, versionStr, FileName), url)
	if err != nil {
		return errors.Wrap(err, "file fetcher create request")
	}

	success := false

	t := time.NewTicker(1 * time.Second)
	defer t.Stop()

	for !success {
		resp := f.grabClient.Do(req)

	Loop:
		for {
			select {
			case <-t.C:
				level.Debug(f.log).Log("msg", fmt.Sprintf("transferred %d / %d bytes (%.2f%%)",
					resp.BytesComplete(),
					resp.Size(),
					100*resp.Progress()))
			case <-resp.Done:
				break Loop
			}
		}

		if err := resp.Err(); err != nil {
			level.Error(f.log).Log("msg", resp.Err().Error())
		} else {
			success = true
		}
	}

	return nil
}
