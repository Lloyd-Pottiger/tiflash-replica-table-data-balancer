package http

import (
	"bytes"
	"fmt"
	"io"
	"net/http"

	"github.com/pkg/errors"
)

// composeURL adds HTTP schema if missing and concats address with path
func composeURL(schema, address, path string) string {
	return fmt.Sprintf("%s://%s%s", schema, address, path)
}

func postJSON(httpClient *http.Client, schema, endpoint, prefix string, data []byte) error {
	url := composeURL(schema, endpoint, prefix)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	msg, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("[%d] %s", resp.StatusCode, msg)
	}
	return nil
}
