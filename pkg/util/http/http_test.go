package http

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

type addTest struct {
	resp   http.Response
	output bool
}

var tests = []addTest{
	{http.Response{StatusCode: 200}, true},
	{http.Response{StatusCode: 102}, false},
	{http.Response{StatusCode: 301}, false},
	{http.Response{StatusCode: 404}, false},
	{http.Response{StatusCode: 500}, false},
}

func TestIsSuccessStatusCode(t *testing.T) {
	for _, v := range tests {
		res := isSuccessStatusCode(&v.resp)
		assert.Equal(t, res, v.output, fmt.Sprintf("output %t not equal to expected %t", res, v.output))
	}
}

func TestEnsureSuccessStatusCode(t *testing.T) {
	for _, v := range tests {
		err := EnsureSuccessStatusCode(&v.resp)
		assert.Equal(t, v.output, err == nil, fmt.Sprintf("output %t not equal to expected %t", err == nil, v.output))
	}
}
