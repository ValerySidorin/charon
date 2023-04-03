package manager

import (
	"context"
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
	FileName = "gar.zip"
)

type Manager struct {
	grabClient *grab.Client
	log        log.Logger
}

func New(bufferSize int, log log.Logger) *Manager {
	c := grab.NewClient()
	c.BufferSize = bufferSize

	return &Manager{
		grabClient: c,
		log:        log,
	}
}

func (f *Manager) Download(path string, version int, url string) error {
	versionStr := strconv.Itoa(version)
	if err := cleanPrevDownload(path, versionStr); err != nil {
		return err
	}

	_ = level.Info(f.log).Log("msg", fmt.Sprintf("start downloading file: %s", url))
	req, err := grab.NewRequest(filepath.Join(path, versionStr, FileName), url)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req = req.WithContext(ctx)
	if err != nil {
		return errors.Wrap(err, "file fetcher create request")
	}

	t := time.NewTicker(1 * time.Second)
	defer t.Stop()

	resp := f.grabClient.Do(req)

	// Sometimes a connection is lost, but we can not properly detect it,
	// so we need to monitor, if file is still downloading
	go func() {
		t2 := time.NewTicker(30 * time.Second)
		defer t2.Stop()

		prevProg := resp.Progress()

		for {
			select {
			case <-t2.C:
				currProg := resp.Progress()
				if currProg == prevProg {
					_ = level.Error(f.log).Log("msg", "seems like an existing connection was forcibly closed by the remote host, canceling context")
					cancel()
				} else {
					prevProg = currProg
				}
			case <-resp.Done:
				return
			}
		}
	}()

Loop:
	for {
		select {
		case <-t.C:
			_ = level.Debug(f.log).Log("msg", fmt.Sprintf("transferred %d / %d bytes (%.2f%%)",
				resp.BytesComplete(),
				resp.Size(),
				100*resp.Progress()), "version", version)
		case <-resp.Done:
			break Loop
		}
	}

	if err := resp.Err(); err != nil {
		_ = level.Error(f.log).Log("msg", resp.Err().Error())
		return err
	}

	return nil
}

func cleanPrevDownload(path string, version string) error {
	if _, err := os.Stat(filepath.Join(path, version)); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return errors.Wrap(err, "file fether os.Stat")
		}
	} else {
		entries, err := os.ReadDir(path)
		if err != nil {
			return errors.Wrap(err, "file fetcher os.ReadDir")
		}

		versionedDir := entries[0]
		if !strings.HasSuffix(versionedDir.Name(), version) {
			if err := os.RemoveAll(path); err != nil {
				return errors.Wrap(err, "file fetcher os.RemoveAll")
			}
			if err := os.MkdirAll(path, os.ModeAppend); err != nil {
				return errors.Wrap(err, "file fetcher os.MkDirAll")
			}
		}
	}

	return nil
}
