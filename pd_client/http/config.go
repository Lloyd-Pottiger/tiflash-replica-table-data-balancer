package http

import (
	"crypto/tls"
	"crypto/x509"
	"log"
	"net/http"
	"os"

	client "github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/pd_client"
	"github.com/pingcap/errors"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

// Security is the security section of the config.
type Security struct {
	SSLCA   string
	SSLCert string
	SSLKey  string
}

// ToTLSConfig generates tls's config based on security section of the config.
func (s *Security) ToTlsConfig() (tlsConfig *tls.Config, err error) {
	if len(s.SSLCA) != 0 {
		certPool := x509.NewCertPool()
		// Create a certificate pool from the certificate authority
		var ca []byte
		ca, err = os.ReadFile(s.SSLCA)
		if err != nil {
			err = errors.Errorf("could not read ca certificate: %s", err)
			return
		}
		// Append the certificates from the CA
		if !certPool.AppendCertsFromPEM(ca) {
			err = errors.New("failed to append ca certs")
			return
		}
		tlsConfig = &tls.Config{
			RootCAs:   certPool,
			ClientCAs: certPool,
		}

		if len(s.SSLCert) != 0 && len(s.SSLKey) != 0 {
			getCert := func() (*tls.Certificate, error) {
				// Load the client certificates from disk
				cert, err := tls.LoadX509KeyPair(s.SSLCert, s.SSLKey)
				if err != nil {
					return nil, errors.Errorf("could not load client key pair: %s", err)
				}
				return &cert, nil
			}
			// pre-test cert's loading.
			if _, err = getCert(); err != nil {
				return
			}
			tlsConfig.GetClientCertificate = func(info *tls.CertificateRequestInfo) (certificate *tls.Certificate, err error) {
				return getCert()
			}
			tlsConfig.GetCertificate = func(info *tls.ClientHelloInfo) (certificate *tls.Certificate, err error) {
				return getCert()
			}
		}
	}
	return
}

type HttpConfig struct {
	Endpoint string
	Security *Security
}

func (c *HttpConfig) GetClient() client.PDClient {
	var opts []pdhttp.ClientOption
	var schema string
	var tlsCfg *tls.Config
	var err error
	if c.Security != nil && (len(c.Security.SSLCA) != 0 || len(c.Security.SSLCert) != 0 || len(c.Security.SSLKey) != 0) {
		tlsCfg, err = c.Security.ToTlsConfig()
		if err != nil {
			log.Fatal("could not load cluster ssl", zap.Error(err))
			os.Exit(1)
		}
		opts = append(opts, pdhttp.WithTLSConfig(tlsCfg))
		schema = "https"
	} else {
		schema = "http"
	}
	return &PDHttp{
		Endpoint: c.Endpoint,
		Client: pdhttp.NewClient(
			"balancer",
			[]string{composeURL(schema, c.Endpoint, "")},
			opts...),
		rawHttpClient: &http.Client{
			Transport: &http.Transport{TLSClientConfig: tlsCfg},
		},
		schema: schema,
	}
}
