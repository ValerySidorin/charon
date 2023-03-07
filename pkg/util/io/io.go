package io

import (
	"bytes"
	"io"

	"github.com/pkg/errors"
)

func TryGetSize(r io.Reader) (int64, error) {
	switch f := r.(type) {
	case *bytes.Reader:
		return int64(f.Len()), nil
	}

	return 0, errors.Errorf("unsupported type of io.Reader: %T", r)
}
