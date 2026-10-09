# certs/

PEM files per environment, read by the `kafka_tls` role on the control
node. Everything here except this file is git-ignored.

```
certs/test/          # generated on the first run while kafka_tls_selfsigned: true
certs/prod/
  ca.pem                          bank CA chain (all certificates, PEM)
  int-res-prod-kafka01.crt        broker certificate, PEM
  int-res-prod-kafka01.key        its private key, PEM, unencrypted, chmod 600
  int-res-prod-kafka02.crt / .key
  int-res-prod-kafka03.crt / .key
```

What to request from the CA (SANs, key usage): see the main README,
section "Certificates". Keep the directory `chmod 700`.
