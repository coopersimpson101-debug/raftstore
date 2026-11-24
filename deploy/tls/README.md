# TLS material (not committed)

raftstore uses mutual TLS between nodes when `tls` is set in the node config. Generate a
private CA and one certificate per node (example with OpenSSL; run on a trusted machine):

```sh
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 3650 -subj "/CN=raftstore-ca" -keyout ca.key -out ca.pem
for n in nas pi5 server; do
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -subj "/CN=$n" -keyout $n.key -out $n.csr
  openssl x509 -req -in $n.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 825 -out $n.pem
done
```

Copy `ca.pem`, `<node>.pem` and `<node>.key` to `/etc/raftstore/tls/` on each machine
(`chmod 600` the key, owned by the `raftstore` user). Keep `ca.key` offline.
With TLS enabled the HTTP API is also served over HTTPS; clients use `https://` URLs.
