package balancer

import (
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/pkg/errors"
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

type Config struct {
	PDEndpoint string
	TableID    int64
	Security   *Security
}

var GlobalConfig *Config
