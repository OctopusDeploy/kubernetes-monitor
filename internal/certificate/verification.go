package certificate

import (
	"crypto/x509"
	"fmt"
	"os"
)

func GetRootCertificatePool(additionalRootBundlePath string) (*x509.CertPool, error) {
	rootCertificates, _ := x509.SystemCertPool()
	if _, err := os.Stat(additionalRootBundlePath); err == nil {
		// Load custom root CA bundle if it exists
		certData, err := os.ReadFile(additionalRootBundlePath)
		if err != nil {
			return rootCertificates, err
		}
		if !rootCertificates.AppendCertsFromPEM(certData) {
			return rootCertificates, fmt.Errorf("failed to append custom root CA bundle from %s",
				additionalRootBundlePath)
		}
	}
	return rootCertificates, nil
}
