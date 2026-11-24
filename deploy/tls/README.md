# TLS material (not committed)

raftstore uses mutual TLS between nodes when `tls` is set in the node config. Generate a
private CA and one certificate per node (example with OpenSSL; run on a trusted machine).

The explicit extensions matter: Python 3.13+ verifies certificates with `VERIFY_X509_STRICT`
by default, which rejects a CA without `keyUsage` and leaf certificates without an authority
key identifier (every handshake fails, so no leader is ever elected).

```sh
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 3650 -subj "/CN=raftstore-ca" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -keyout ca.key -out ca.pem
for n in nas pi5 server; do
  cat > $n.ext <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth,clientAuth
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid
subjectAltName=DNS:$n
EOF
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -subj "/CN=$n" -keyout $n.key -out $n.csr
  openssl x509 -req -in $n.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 825 -extfile $n.ext -out $n.pem
done
```

Copy `ca.pem`, `<node>.pem` and `<node>.key` to `/etc/raftstore/tls/` on each machine
(`chmod 600` the key, owned by the `raftstore` user). Keep `ca.key` offline.
With TLS enabled the HTTP API is also served over HTTPS; clients use `https://` URLs.
