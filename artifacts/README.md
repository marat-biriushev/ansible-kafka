# artifacts/

Offline files for the roles. Filled from the bundle made by
`scripts/fetch-artifacts.sh` on a machine with internet access:

```bash
tar -xzf ansible-kafka-artifacts-<date>.tar.gz -C artifacts/
scripts/verify-artifacts.sh
ansible-galaxy collection install artifacts/collections/*.tar.gz -p ./collections
```

Expected content (versions from `artifacts.yml`):

```
kafka_2.13-4.3.1.tgz
jmx_prometheus_javaagent-1.7.0.jar
node_exporter-1.12.1.linux-amd64.tar.gz
collections/community-crypto-3.5.0.tar.gz
collections/community-docker-5.4.0.tar.gz
rpms/                  # optional, fetch-artifacts.sh --with-java
SHA256SUMS
```

Everything here except this file is git-ignored.
