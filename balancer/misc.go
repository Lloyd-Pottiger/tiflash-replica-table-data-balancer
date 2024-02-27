package balancer

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/pkg/errors"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

var (
	internalClientInit sync.Once
	internalHTTPClient *http.Client
	internalHTTPSchema string
)

// InternalHTTPClient is used by TiDB-Server to request other components.
func InternalHTTPClient() *http.Client {
	internalClientInit.Do(initInternalClient)
	return internalHTTPClient
}

func initInternalClient() {
	var tlsCfg *tls.Config
	var err error
	if GlobalConfig.Security != nil {
		tlsCfg, err = GlobalConfig.Security.ToTlsConfig()
		if err != nil {
			log.Fatal("could not load cluster ssl", zap.Error(err))
		}
	}
	if tlsCfg == nil {
		internalHTTPSchema = "http"
		internalHTTPClient = http.DefaultClient
		return
	}
	internalHTTPSchema = "https"
	internalHTTPClient = &http.Client{
		Transport: &http.Transport{TLSClientConfig: tlsCfg},
	}
}

// InternalHTTPSchema specifies use http or https to request other components.
func InternalHTTPSchema() string {
	internalClientInit.Do(initInternalClient)
	return internalHTTPSchema
}

// composeURL adds HTTP schema if missing and concats address with path
func ComposeURL(address, path string) string {
	if strings.HasPrefix(address, "http://") || strings.HasPrefix(address, "https://") {
		return fmt.Sprintf("%s%s", address, path)
	}
	return fmt.Sprintf("%s://%s%s", InternalHTTPSchema(), address, path)
}

func PostJSON(endpoint, prefix string, data []byte) error {
	url := ComposeURL(endpoint, prefix)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := InternalHTTPClient().Do(req)
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

var pdClient pdhttp.Client

func InitPDClient() {
	var opts []pdhttp.ClientOption
	if GlobalConfig.Security != nil {
		tlsCfg, err := GlobalConfig.Security.ToTlsConfig()
		if err != nil {
			log.Fatal("could not load cluster ssl", zap.Error(err))
			os.Exit(1)
		}
		opts = append(opts, pdhttp.WithTLSConfig(tlsCfg))
	}
	pdClient = pdhttp.NewClient(
		"lightning-ctl",
		[]string{ComposeURL(GlobalConfig.PDEndpoint, "")},
		opts...)
}
