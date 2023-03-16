package http

import (
	"errors"
	"net/http"
)

func isSuccessStatusCode(resp *http.Response) bool {
	return resp.StatusCode >= 200 && resp.StatusCode <= 299
}

func EnsureSuccessStatusCode(resp *http.Response) error {
	if !isSuccessStatusCode(resp) {
		return errors.New("http response did not indicate success status code: " + resp.Status)
	}
	return nil
}
