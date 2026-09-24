package certificate_test

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"testing"

	"github.com/OctopusDeploy/octopus-grpc/go/pkg/certificate"

	certpool "github.com/octopusdeploy/kubernetes-monitor/internal/certificate"
)

const (
	rootCaFilePath                  = "../../testdata/certs/rootCA.cert.pem"
	clientSignedBySubChainFilePath  = "../../testdata/certs/client_sub_chain.cert.pem"
	clientSignedByRootChainFilePath = "../../testdata/certs/client_root_chain.cert.pem"
)

func TestRootCertificatePool_ReturnsPemEncodedRoot(t *testing.T) {
	// arrange
	rootCas, err := getCertificates(rootCaFilePath)
	if err != nil {
		t.Fatalf("Expected no error getting root CA certificate, got %v", err)
	}

	// act
	pool, err := certpool.GetRootCertificatePool("../../testdata/certs/rootCA.cert.pem")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// assert
	found := false
	rootCa := rootCas[0] // Get the first certificate as the root CA
	for _, subj := range pool.Subjects() {
		if bytes.Compare(subj, rootCa.RawSubject) == 0 {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected root CA certificate to be in the pool, but it was not found")
	}
}

func TestVerifyServerCertificate_CorrectlyVerifies_WhenUsingSubCaSignedCert_UsingCustomRootCa(t *testing.T) {
	// arrange
	clientCerts, err := getCertificates(clientSignedBySubChainFilePath)
	if err != nil {
		t.Fatalf("Expected no error getting client certificates, got %v", err)
	}
	if len(clientCerts) != 3 {
		t.Fatalf("Expected 3 certificates, got %d", len(clientCerts))
	}
	rootBundle, err := certpool.GetRootCertificatePool(rootCaFilePath)
	if err != nil {
		t.Fatalf("Expected no error getting root CA bundle, got %v", err)
	}

	// act/assert
	err = certificate.VerifyServerCertificateWithRoot(clientCerts, rootBundle)
	if err != nil {
		t.Fatalf("Expected no error verifying server certificate, got %v", err)
	}
}

func TestVerifyServerCertificate_DoesNotVerify_WhenUsingSubCaSignedCert_NotUsingCustomRootCa(t *testing.T) {
	// arrange
	clientCerts, err := getCertificates(clientSignedBySubChainFilePath)
	if err != nil {
		t.Fatalf("Expected no error getting client certificates, got %v", err)
	}
	if len(clientCerts) != 3 {
		t.Fatalf("Expected 3 certificates, got %d", len(clientCerts))
	}
	rootBundle, err := certpool.GetRootCertificatePool("")
	if err != nil {
		t.Fatalf("Expected no error getting root CA bundle, got %v", err)
	}

	// act/assert
	err = certificate.VerifyServerCertificateWithRoot(clientCerts, rootBundle)
	if err == nil {
		t.Fatal("Expected error verifying server certificate, got none")
	}
}

func TestVerifyServerCertificate_CorrectlyVerifies_WhenUsingRootCaSignedCert_UsingCustomRootCa(t *testing.T) {
	// arrange
	clientCerts, err := getCertificates(clientSignedByRootChainFilePath)
	if err != nil {
		t.Fatalf("Expected no error getting client certificates, got %v", err)
	}
	if len(clientCerts) != 2 {
		t.Fatalf("Expected 2 certificates, got %d", len(clientCerts))
	}
	rootBundle, err := certpool.GetRootCertificatePool(rootCaFilePath)
	if err != nil {
		t.Fatalf("Expected no error getting root CA bundle, got %v", err)
	}

	// act/assert
	err = certificate.VerifyServerCertificateWithRoot(clientCerts, rootBundle)
	if err != nil {
		t.Fatalf("Expected no error verifying server certificate, got %v", err)
	}
}

func getCertificates(filePath string) ([]*x509.Certificate, error) {
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read certificate file: %w", err)
	}
	var certs []*x509.Certificate
	for block, rest := pem.Decode(fileData); block != nil; block, rest = pem.Decode(rest) {
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse certificate: %w", err)
		}
		certs = append(certs, cert)
	}
	return certs, nil
}
