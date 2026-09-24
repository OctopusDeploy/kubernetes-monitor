#!/usr/bin/env bash
set -e

# -------------------------------------------------------------------
# gencerts.sh
# Script to generate a Root CA, Subordinate CA, and client certificates for testing.
# You should only need to run this once and commit the generated certs in the parent directory.
# -------------------------------------------------------------------

ROOT_SUBJ="/C=AU/ST=QLD/O=Octopus/OU=RootCA/CN=My Root CA/emailAddress=root@okm.octopus.local"
SUBCA_SUBJ="/C=AU/ST=QLD/O=Octopus/OU=SubCA/CN=My Subordinate CA/emailAddress=subca@okm.octopus.local"
CLIENT_SUB_SUBJ="/C=AU/ST=QLD/O=Octopus/OU=Clients/CN=SubCA Client/emailAddress=client_sub@okm.octopus.local"
CLIENT_ROOT_SUBJ="/C=AU/ST=QLD/O=Octopus/OU=Clients/CN=RootCA Client/emailAddress=client_root@okm.octopus.local"

# Lifetime in days (~100 years)
DAYS=36500

# Filenames
ROOT_KEY=rootCA.key.pem
ROOT_CERT=rootCA.cert.pem
SUB_KEY=subCA.key.pem
SUB_CSR=subCA.csr.pem
SUB_CERT=subCA.cert.pem
CLIENT_SUB_KEY=client_sub.key.pem
CLIENT_SUB_CSR=client_sub.csr.pem
CLIENT_SUB_CERT=client_sub.cert.pem
CLIENT_SUB_CHAIN=client_sub_chain.cert.pem
CLIENT_ROOT_KEY=client_root.key.pem
CLIENT_ROOT_CSR=client_root.csr.pem
CLIENT_ROOT_CERT=client_root.cert.pem
CLIENT_ROOT_CHAIN=client_root_chain.cert.pem


# -------------------------------------------------------------------
# 1) Generate Root CA key & self-signed cert (no prompts)
# -------------------------------------------------------------------
openssl genrsa -out $ROOT_KEY 4096
openssl req -x509 -config root.cnf \
    -key $ROOT_KEY \
    -new -sha256 \
    -days $DAYS \
    -extensions v3_ca \
    -subj "$ROOT_SUBJ" \
    -out $ROOT_CERT

# -------------------------------------------------------------------
# 2) Generate Sub CA key, CSR, and sign with Root CA
# -------------------------------------------------------------------
openssl genrsa -out $SUB_KEY 4096
openssl req -new -config subca.cnf \
    -key $SUB_KEY \
    -subj "$SUBCA_SUBJ" \
    -out $SUB_CSR

openssl x509 -req \
    -in $SUB_CSR \
    -CA $ROOT_CERT \
    -CAkey $ROOT_KEY \
    -CAcreateserial \
    -extensions v3_intermediate_ca \
    -extfile subca.cnf \
    -days $DAYS \
    -sha256 \
    -out $SUB_CERT

# -------------------------------------------------------------------
# 3) Generate client cert signed by Sub CA + full chain
# -------------------------------------------------------------------
openssl genrsa -out $CLIENT_SUB_KEY 2048
openssl req -new -config subca.cnf \
    -key $CLIENT_SUB_KEY \
    -subj "$CLIENT_SUB_SUBJ" \
    -out $CLIENT_SUB_CSR

openssl x509 -req \
    -in $CLIENT_SUB_CSR \
    -CA $SUB_CERT \
    -CAkey $SUB_KEY \
    -CAcreateserial \
    -extensions usr_cert \
    -extfile subca.cnf \
    -days $DAYS \
    -sha256 \
    -out $CLIENT_SUB_CERT

# bundle full chain for Sub-CA client
cat $CLIENT_SUB_CERT $SUB_CERT $ROOT_CERT > $CLIENT_SUB_CHAIN

# -------------------------------------------------------------------
# 4) Generate client cert signed directly by Root CA + chain
# -------------------------------------------------------------------
openssl genrsa -out $CLIENT_ROOT_KEY 2048
openssl req -new -config root.cnf \
    -key $CLIENT_ROOT_KEY \
    -subj "$CLIENT_ROOT_SUBJ" \
    -out $CLIENT_ROOT_CSR

openssl x509 -req \
    -in $CLIENT_ROOT_CSR \
    -CA $ROOT_CERT \
    -CAkey $ROOT_KEY \
    -CAcreateserial \
    -extensions usr_cert \
    -extfile root.cnf \
    -days $DAYS \
    -sha256 \
    -out $CLIENT_ROOT_CERT

# bundle chain for root client
cat $CLIENT_ROOT_CERT $ROOT_CERT > $CLIENT_ROOT_CHAIN

# -------------------------------------------------------------------
# 5) Copy public certificates into the parent directory
# -------------------------------------------------------------------
for cert in $ROOT_CERT $SUB_CERT $CLIENT_SUB_CERT $CLIENT_ROOT_CERT $CLIENT_SUB_CHAIN $CLIENT_ROOT_CHAIN; do
  cp "$cert" "../"
done

echo "✅ All certs generated"
echo "Public keys are in the parent directory."