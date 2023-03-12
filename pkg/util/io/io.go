package io

import (
	"bytes"
	"io"
	"os"

	"github.com/pkg/errors"
)

func TryGetSize(r io.Reader) (int64, error) {
	switch f := r.(type) {
	case *bytes.Reader:
		return int64(f.Len()), nil
	case *os.File:
		filestat, err := f.Stat()
		if err != nil {
			return 0, err
		}
		return filestat.Size(), nil
	}

	return 0, errors.Errorf("unsupported type of io.Reader: %T", r)
}
