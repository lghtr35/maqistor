#!/usr/bin/env sh
set -eu
export MSYS_NO_PATHCONV=1
cert_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/certs"
mkdir -p "$cert_dir"
cd "$cert_dir"
openssl genrsa -out ca-key.pem 2048
openssl req -x509 -new -key ca-key.pem -sha256 -days 30 -out ca.pem -subj '/CN=maqistor-benchmark-ca'
openssl genrsa -out maqistor-key.pem 2048
openssl req -new -key maqistor-key.pem -out maqistor.csr -subj '/CN=maqistor-benchmark'
printf 'subjectAltName=DNS:maqistor-benchmark,DNS:host.docker.internal\nextendedKeyUsage=serverAuth\n' > server.ext
openssl x509 -req -in maqistor.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial -out maqistor-cert.pem -days 30 -sha256 -extfile server.ext
openssl genrsa -out worker-key.pem 2048
openssl req -new -key worker-key.pem -out worker.csr -subj '/CN=maqistor-benchmark-worker'
printf 'extendedKeyUsage=clientAuth\n' > worker.ext
openssl x509 -req -in worker.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial -out worker-cert.pem -days 30 -sha256 -extfile worker.ext
rm -f *.csr *.ext *.srl
