package io

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

type addTest struct {
	reader io.Reader
	len    int64
	isErr  bool
}

var (
	br    = bytes.NewReader([]byte("12345"))
	tests = []addTest{
		{br, 5, false},
		{nil, 0, true},
	}
)

func TestTryGetSize(t *testing.T) {
	for _, v := range tests {
		res, err := TryGetSize(v.reader)
		assert.Equal(t, v.len, res, fmt.Sprintf("output len %d not equal to expected %d", res, v.len))
		assert.Equal(t, v.isErr, err != nil, "output err is not valid")
	}
}
