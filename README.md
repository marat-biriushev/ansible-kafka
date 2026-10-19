# ansible-kafka

Apache Kafka 4.3.1 (KRaft, combined broker+controller on 3 nodes) and its
service tools for Integration on RHEL 10, TEST and PROD. Everything runs
from an isolated control node: no task reaches the internet, all binaries
come from `artifacts/`, container images from the internal Nexus mirror.
The control node needs only `ansible-core` 2.15 or newer and `openssl`:
no Ansible collections.

Scope of this repository:

* **In:** Kafka itself, TLS stores, SCRAM service accounts and their ACLs,
  service topics, JMX exporter, node_exporter, Schema Registry / Kafka
  Connect / Kafka UI / Redpanda Console / kafka-exporter / canary in Docker, rolling restart,
  monitoring rules.
* **Out, done by hand:** everything at OS level (data disk, time sync,
  sysctl, firewall, SELinux, Java and other packages), see
  [OS preparation](#os-preparation). Docker on the services VM is
  installed with the `docker` role from ansible-ib.
* **Out, other repositories / phase 2:** application topics and ACLs (CI/CD
  repo), connector definitions (created through the Connect REST API or
  Kafka UI), MirrorMaker 2, DR, Cruise Control, vmagent itself.

Every command below is run by the owner on the control node or the hosts,
each with what to check after it.

## Contents

- [Layout](#layout)
- [Hosts](#hosts)
- [OS preparation](#os-preparation)
- [Offline artifacts](#offline-artifacts)
- [Cluster and directory IDs](#cluster-and-directory-ids)
- [Certificates](#certificates)
- [Secrets](#secrets)
- [Deployment from scratch](#deployment-from-scratch)
- [Kafka Connect and database connectors](#kafka-connect-and-database-connectors)
- [Playbooks](#playbooks)
- [Rolling restart](#rolling-restart)
- [Manual tests on TEST](#manual-tests-on-test)
- [Monitoring](#monitoring)
- [broker.rack](#brokerrack)
- [Checks and molecule](#checks-and-molecule)
- [Default decisions](#default-decisions)
- [Known limitations](#known-limitations)

## Layout

```
ansible.cfg                  roles_path, pipelining
artifacts.yml                offline artifacts: version, URL, checksum
artifacts/                   downloaded files (git-ignored except README.md)
certs/<env>/                 PEM files per environment (git-ignored except README.md)
scripts/
  fetch-artifacts.sh         machine WITH internet: download + bundle
  verify-artifacts.sh        control node: check the unpacked bundle
  check-offline.sh           CI / pre-commit: roles do not reach the internet
inventories/{test,prod}/
  hosts.yml                  groups kafka, kafka_services
  host_vars/<host>.yml       ansible_host, kafka_node_id, kafka_rack, kafka_directory_id
  group_vars/all/main.yml    everything else; secrets only by reference
  group_vars/all/vault.yml.example
playbooks/
  site.yml                   everything from scratch
  kafka.yml                  cluster only (+ rolling restart of changed nodes)
  users-acl.yml              service topics, SCRAM accounts, ACLs
  services.yml               services VM
  rolling-restart.yml        one node at a time, controller last
roles/
  kafka_tls                  PEM -> PKCS12 stores; CA for clients; self-signed CA
  kafka_jmx_exporter         javaagent jar + rules
  kafka                      install, configure, format once, start, wait
  node_exporter              node_exporter under systemd
  kafka_topics               service topics
  kafka_users                SCRAM accounts and ACLs
  schema_registry            Docker compose
  kafka_connect              Docker compose, plugins from artifacts/connect-plugins/
  kafka_ui                   Docker compose
  redpanda_console           Docker compose, behind an nginx login form
  kafka_exporter             Docker compose
  kafka_canary               Docker compose
monitoring/
  vmagent-scrape.yml         scrape_configs fragment for the existing vmagent
  kafka-alerts.yml           vmalert rules
molecule/default/            3-node scenario (developer machine)
```

Every role has a README with the full variable list, `defaults/main.yml`
and `meta/argument_specs.yml` (checked when the role starts).

## Hosts

Domain `ipa.ipotekabank.uz`. Inventory hostnames are short names, the FQDN
is `<name>.ipa.ipotekabank.uz`.

| Env | Host | IP | Role |
|---|---|---|---|
| TEST | int-res-test-kafka01..03 | 172.31.56.211-213 | broker+controller, node.id 1..3 |
| TEST | int-res-test-kafkasvc01 | 172.31.56.214 | services |
| PROD | int-res-prod-kafka01..03 | 172.31.60.111-113 | broker+controller, node.id 1..3 |
| PROD | int-res-prod-kafkasvc01 | 172.31.60.114 | services |

Client bootstrap name (DNS, one A record per broker, created outside this
repo): `int-res-<env>-kafka.ipa.ipotekabank.uz`. It is in the certificate
SANs.

## OS preparation

The roles do not change the OS. Do the following by hand on each host
before the first run; the roles do not check it either, so a missed step
shows up later as a Kafka error.

### Data disk (kafka01-03)

Kafka keeps its log segments in `/data/kafka`, which must be the separate
data disk: a full OS disk would take the node down, and the disk is sized
for the retention. XFS is what the Kafka docs recommend for production;
`noatime` saves a metadata write on every segment read; the UUID in fstab
survives device renaming.

```bash
lsblk                                   # find the data disk, e.g. /dev/sdb; check it has no filesystem
blkid /dev/sdb || mkfs.xfs /dev/sdb     # only if blkid prints nothing - never reformat a disk in use
mkdir -p /data
echo "UUID=$(blkid -s UUID -o value /dev/sdb) /data xfs defaults,noatime 0 0" >> /etc/fstab
systemctl daemon-reload && mount /data
findmnt /data                            # check: xfs, noatime, the data disk
```

### Time (all hosts)

KRaft, TLS certificate validation and log timestamps need correct clocks.

```bash
dnf install -y chrony
# server <ntp-server> iburst  lines in /etc/chrony.conf
systemctl enable --now chronyd
chronyc tracking                          # check: "Leap status : Normal", small offset
```

### sysctl (kafka01-03)

```bash
cat > /etc/sysctl.d/90-kafka.conf <<'EOF'
vm.swappiness = 1
vm.dirty_ratio = 80
vm.dirty_background_ratio = 5
net.core.somaxconn = 4096
net.ipv4.tcp_max_syn_backlog = 4096
EOF
sysctl --system
sysctl vm.swappiness vm.max_map_count     # check: swappiness 1, max_map_count >= 262144
```

`vm.max_map_count` is not in the file on purpose: RHEL 10 already sets it to
1048576 (`/usr/lib/sysctl.d/10-map-count.conf`), above what Kafka needs.
Add `vm.max_map_count = 262144` only on a host where it is lower.

| Setting | Why |
|---|---|
| `vm.swappiness=1` | keep the JVM heap in RAM; swapping a broker stalls it and drops it out of the ISR |
| `vm.dirty_ratio=80`, `vm.dirty_background_ratio=5` | Kafka relies on the page cache; flush in the background early, block writers only very late |
| `vm.max_map_count` >= 262144 | every log segment index is mmapped; the kernel default 65530 runs out with many partitions (RHEL 10 ships 1048576) |
| `net.core.somaxconn`, `net.ipv4.tcp_max_syn_backlog = 4096` | room for connection bursts from many clients and after a restart |

The open files limit (100000) is set by the `kafka` role in
`kafka.service` (`LimitNOFILE`), nothing to do here.

### Firewall (all hosts)

firewalld stays on. Open:

| Port | From | Host |
|---|---|---|
| 9092/tcp | client networks, kafkasvc01 | kafka01-03 |
| 9091/tcp, 9093/tcp | the other two kafka nodes only | kafka01-03 |
| 7071/tcp, 9100/tcp | monitoring (vmagent) | all hosts |
| 8081/tcp (Schema Registry), 8080/tcp (Kafka UI) | clients / users | kafkasvc01 |
| 8090/tcp (Redpanda Console via nginx) | developers | kafkasvc01 |
| 8083/tcp (Kafka Connect REST, no authentication) | operators only; Kafka UI runs on the same host | kafkasvc01 |
| 7071/tcp, 7072/tcp (Schema Registry, Connect JMX exporter) | monitoring | kafkasvc01 |
| 9308/tcp (kafka-exporter), 8082/tcp (canary) | monitoring | kafkasvc01 |

Example on a broker (TEST addresses; replace the networks):

```bash
for peer in 172.31.56.211 172.31.56.212 172.31.56.213; do
  firewall-cmd --permanent --add-rich-rule="rule family=ipv4 source address=${peer} port port=9091 protocol=tcp accept"
  firewall-cmd --permanent --add-rich-rule="rule family=ipv4 source address=${peer} port port=9093 protocol=tcp accept"
done
firewall-cmd --permanent --add-rich-rule='rule family=ipv4 source address=<client-net>/<prefix> port port=9092 protocol=tcp accept'
firewall-cmd --permanent --add-rich-rule='rule family=ipv4 source address=<monitoring-net>/<prefix> port port=7071 protocol=tcp accept'
firewall-cmd --permanent --add-rich-rule='rule family=ipv4 source address=<monitoring-net>/<prefix> port port=9100 protocol=tcp accept'
firewall-cmd --reload && firewall-cmd --list-rich-rules
```

Docker on kafkasvc01 publishes its ports through its own iptables/nftables
chains; check with `ss -ltn` and a connection from another host after the
services are up.

### SELinux

Stays `enforcing`. Kafka and node_exporter run from `/opt` under systemd
and need no extra policy. Docker's default `daemon.json` (ansible-ib) does
not enable SELinux labelling for containers, so the read-only bind mounts
need no relabelling. Check after deployment: `ausearch -m avc -ts recent`
shows nothing for kafka/node_exporter.

### Packages

From the internal dnf repository (Satellite / mirror); check that it
carries them before the first run:

| Package | Where | Why |
|---|---|---|
| `java-21-openjdk-headless` | kafka01-03 | runs Kafka; `keytool` builds the truststore |
| `openssl` | kafka01-03 | builds the keystore (in the base OS) |
| `chrony` | all | time sync |
| `firewalld` | all | firewall |
| `python3` | all | Ansible modules (in the base OS) |
| `docker-ce`, `docker-compose-plugin` | kafkasvc01 | installed by the `docker` role from ansible-ib |
| `openssl` (3.0+) | control node | generates the self-signed CAs and certificates |

```bash
dnf install -y java-21-openjdk-headless openssl
java -version                             # check: 21
```

If the repository has no Java 21: `scripts/fetch-artifacts.sh --with-java`
on an Internet-connected RHEL 10 / AlmaLinux 10 machine puts the RPMs and
their dependencies into `artifacts/rpms/`; install them with
`dnf install ./artifacts/rpms/*.rpm` on each broker.

## Offline artifacts

`artifacts.yml` lists what the roles need from outside, with pinned
versions and checksums:

| Artifact | Version | Used on |
|---|---|---|
| `kafka_2.13-4.3.1.tgz` | 4.3.1 (sha512) | kafka01-03 |
| `jmx_prometheus_javaagent-1.7.0.jar` | 1.7.0 (sha256) | kafka01-03, kafkasvc01 |
| `node_exporter-1.12.1.linux-amd64.tar.gz` | 1.12.1 (sha256) | all hosts |
| Java 21 RPMs (optional) | from the RHEL/Alma repos | kafka01-03 |

Container images are not in the bundle: the services VM pulls them through
`docker-proxy.nexus.otp.ipotekabank.uz` (see the Docker role in ansible-ib).

1. On a machine **with** internet (needs curl, python3 with PyYAML):

   ```bash
   git clone <this repo> && cd ansible-kafka
   scripts/fetch-artifacts.sh            # add --with-java on RHEL/Alma 10 if needed
   ```

   It skips files that are already there with the right checksum, fails on
   a mismatch, and prints the file list, the checksums and the bundle size.
   Result: `ansible-kafka-artifacts-<date>.tar.gz`.

2. Copy the bundle to the control node and unpack it:

   ```bash
   tar -xzf ansible-kafka-artifacts-<date>.tar.gz -C artifacts/
   scripts/verify-artifacts.sh           # check: "All artifacts present", exit 0
   ```

Without a machine to run the fetch script on, download the files by hand
(any browser) from the `url` of each entry in `artifacts.yml`, put them
into `artifacts/` under the same names, and create the checksum list
yourself; `verify-artifacts.sh` still checks every file against the
pinned checksums:

```bash
(cd artifacts && find . -type f ! -name SHA256SUMS ! -name README.md | sort | xargs sha256sum > SHA256SUMS)
scripts/verify-artifacts.sh
```

Each role starts by checking its files in `artifacts/` (and the checksum
from `artifacts.yml`) and stops with "Put X in artifacts/, see README" if
one is missing.

To bump a version: change it in `artifacts.yml` (version, URL, checksum)
and in the role defaults, rerun the fetch script.

## Cluster and directory IDs

`kafka_cluster_id` (group_vars) and one `kafka_directory_id` per node
(host_vars) are generated **once** and committed. The role never generates
them: a new ID on a rerun would mean a different cluster. Without them the
first task stops with this hint.

```bash
tar -xzf artifacts/kafka_2.13-4.3.1.tgz -C /tmp
for i in cluster kafka01 kafka02 kafka03; do echo "$i $(/tmp/kafka_2.13-4.3.1/bin/kafka-storage.sh random-uuid)"; done
```

(needs Java on the control node; otherwise run it on one broker after
installing Java). Put the cluster value into
`inventories/<env>/group_vars/all/main.yml` and each node value into its
`host_vars` file. TEST and PROD get different values.

On formatting, the same `--initial-controllers` string
(`1@<kafka01 fqdn>:9093:<dir1>,2@...,3@...`) is used on all three nodes,
which builds the dynamic quorum (KIP-853). If `/data/kafka/meta.properties`
already exists with a different cluster, node or directory ID, the role
fails instead of touching the disk.

## Certificates

Per environment, in `certs/<env>/` on the control node (git-ignored):
`ca.pem` (bank CA chain), `<host>.crt`, `<host>.key` for each broker, e.g.
`certs/prod/int-res-prod-kafka01.crt`. The `kafka_tls` role builds the
PKCS12 keystore and truststore on the brokers from them.

What to request from the bank CA, per broker:

| Field | Value (kafka01 on PROD as example) |
|---|---|
| CN | `int-res-prod-kafka01.ipa.ipotekabank.uz` |
| SAN DNS | `int-res-prod-kafka01.ipa.ipotekabank.uz`, `int-res-prod-kafka01`, `int-res-prod-kafka.ipa.ipotekabank.uz` |
| SAN IP | `172.31.60.111` |
| Key usage | digitalSignature, keyEncipherment |
| Extended key usage | **serverAuth and clientAuth** (brokers authenticate each other with the same certificate) |
| Key | RSA 3072+ or ECDSA P-256, delivered as unencrypted PEM |

The CN must be the FQDN: it becomes the broker's principal and is listed
in `super.users`. The kafkasvc01 host needs no certificate, only `ca.pem`
(the services authenticate with SCRAM).

TEST and PROD both run with `kafka_tls_selfsigned: true` (owner's
decision; set in each `inventories/<env>/group_vars/all/main.yml`): the
first run generates a CA ("Kafka TEST CA" / "Kafka PROD CA") and the three
certificates with the same names into `certs/<env>/`. Each environment has
its own CA, so a TEST certificate is not trusted by PROD. Clients need
`certs/<env>/ca.pem`; `ca.key` never leaves the control node (back it up
with the vault). To move to the bank CA later: set `false`, put the bank
files into `certs/<env>/` and run `playbooks/kafka.yml` (the truststores are
rebuilt and the brokers restarted one by one); clients must trust the new
CA before that.

Rotation: replace the files in `certs/<env>/` and run
`playbooks/kafka.yml`; the stores are rebuilt and the brokers restarted
one by one.

## Secrets

All passwords are in `inventories/<env>/group_vars/all/vault.yml`
(ansible-vault). Only `vault.yml.example` is committed.

```bash
cd inventories/test/group_vars/all
cp vault.yml.example vault.yml && vi vault.yml     # 12+ characters; none of , [ ] " = $ # \ ` or spaces
ansible-vault encrypt vault.yml
```

`main.yml` refers to the vault values in one place (`kafka_admin_password:
"{{ vault_kafka_admin_password }}"`, ...). To move to HashiCorp Vault
later, replace those right-hand sides with `community.hashi_vault`
lookups; the roles do not change. Tasks that handle passwords have
`no_log: true`. Run playbooks with `--ask-vault-pass` (or
`--vault-password-file`).

## Deployment from scratch

From the repository root (so `ansible.cfg` is used), TEST as example.

0. OS preparation done, artifacts verified, IDs
   and vault filled in, certificates in place (or self-signed, generated on the first run).
   Check reachability:

   ```bash
   ansible -i inventories/test/hosts.yml all -m ping --ask-vault-pass
   ```

1. Dry run, then the whole thing:

   ```bash
   ansible-playbook -i inventories/test/hosts.yml playbooks/site.yml --ask-vault-pass --check --diff
   ansible-playbook -i inventories/test/hosts.yml playbooks/site.yml --ask-vault-pass
   ```

   `--check` on a fresh host cannot get past the storage format and the
   first start (nothing is installed yet); it is useful from the second
   run on.

2. Check on any broker:

   ```bash
   systemctl status kafka
   /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server $(hostname -f):9091 \
     --command-config /etc/kafka/admin.properties describe --status        # 3 voters, a LeaderId
   /opt/kafka/bin/kafka-topics.sh --bootstrap-server $(hostname -f):9091 \
     --command-config /etc/kafka/admin.properties --describe --under-replicated-partitions   # empty
   curl -s localhost:7071/metrics | grep activecontrollercount
   ```

   On kafkasvc01: `docker ps` shows the service containers healthy
   (on TEST: schema-registry, kafka-connect, kafka-ui, redpanda-console,
   redpanda-console-nginx, kafka-exporter; the canary is off);
   `curl -s localhost:8081/subjects` returns `[]`;
   `curl -s localhost:8083/` returns the Connect version and the cluster ID;
   Kafka UI opens on `http://int-res-test-kafkasvc01.ipa.ipotekabank.uz:8080`
   and shows the cluster, Schema Registry and Kafka Connect; on TEST
   `curl -s -o /dev/null -w '%{http_code}' localhost:8090/` returns 302
   (Redpanda Console sends the browser to its login form).

3. Run `site.yml` again: it must end with `changed=0` everywhere.

Order inside `site.yml`: `kafka.yml` (kafka_tls, kafka_jmx_exporter,
kafka, node_exporter on the brokers, then a rolling restart of nodes whose
config changed) -> `users-acl.yml` (kafka_topics, kafka_users on kafka01)
-> `services.yml` (kafka_tls for the CA, node_exporter, Schema Registry,
Kafka Connect, Kafka UI, kafka-exporter and the canary on kafkasvc01).

## Kafka Connect and database connectors

Kafka Connect runs as one distributed worker on kafkasvc01
(`roles/kafka_connect/README.md`). Apache Kafka ships no database
connectors; to add one (for example Debezium for CDC, or a JDBC connector):

1. Add it to `connect_plugins` in `artifacts.yml` (name, version, file,
   url, sha256), rerun `scripts/fetch-artifacts.sh` on the machine with
   internet and bring the bundle over as usual.
2. Add `{name: <plugin name>}` to `kafka_connect_plugins` in
   `inventories/<env>/group_vars/all/main.yml`.
3. Put the database password (and the integration's Kafka password, if the
   connector uses its own account) into `vault_kafka_connect_connector_secrets`.
4. `ansible-playbook -i inventories/<env>/hosts.yml playbooks/services.yml --ask-vault-pass`.
   Check: `curl -s localhost:8083/connector-plugins` on kafkasvc01 lists
   the connector class.
5. Create the connector through Kafka UI or `PUT /connectors/<name>/config`,
   with secrets as `${file:/etc/kafka-connect/secrets/connectors.properties:<key>}`.
   Check: `GET /connectors/<name>/status` is RUNNING for the connector
   and every task.

The source database side (a replication slot and publication for
Debezium on PostgreSQL, LogMiner / supplemental logging on Oracle, a
read-only user...) is set up by the DBAs.

## Playbooks

| Command | What |
|---|---|
| `ansible-playbook -i inventories/<env>/hosts.yml playbooks/site.yml --ask-vault-pass` | everything |
| `... playbooks/kafka.yml ...` | cluster only; restarts changed nodes one by one |
| `... playbooks/users-acl.yml ...` | service topics, SCRAM accounts, ACLs |
| `... playbooks/services.yml ...` | services VM only |
| `... playbooks/rolling-restart.yml ...` | restart all brokers one by one |
| `... playbooks/rolling-restart.yml --check` | show the restart order only |

Add `--limit int-res-test-kafka02` to `kafka.yml` to work on one node
(the rolling restart part then also covers only that node).

## Rolling restart

`playbooks/rolling-restart.yml`, `serial: 1`:

1. Finds the active controller (`LeaderId` from `describe --status`) and
   puts it last.
2. Pre-check: no under-replicated and no unavailable partitions, 3 voters
   with lag <= `kafka_rr_max_lag` (1000). Otherwise it stops.
3. For each node: the same check, `systemctl restart kafka`, wait for
   9091/9092, then wait until URP is empty and the node is back in the
   quorum (`kafka_rr_timeout`, 900 s by default).
4. After the last node: `kafka-leader-election --election-type preferred
   --all-topic-partitions`.

`--check` prints `Restart order (active controller last): a -> b -> c` and
restarts nothing. Override the timeout with `-e kafka_rr_timeout=1800`.

Tested on a local 3-node Kafka 4.3.1 cluster with this exact playbook (the
restart step swapped for a process restart): `kafka-producer-perf-test`
with `acks=all` at 1500 msg/s ran through the whole restart, 250 000
records sent, no errors, only retriable `NOT_LEADER_OR_FOLLOWER` warnings
while leadership moved.

## Manual tests on TEST

Client config for the tests (`/tmp/client.properties` on a broker, with a
SCRAM account that may write the test topic, e.g. admin):

```properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="<vault>";
ssl.truststore.type=PEM
ssl.truststore.location=/etc/kafka/tls/ca.pem
acks=all
```

```bash
B=int-res-test-kafka01.ipa.ipotekabank.uz:9092,int-res-test-kafka02.ipa.ipotekabank.uz:9092,int-res-test-kafka03.ipa.ipotekabank.uz:9092
/opt/kafka/bin/kafka-topics.sh --bootstrap-server $B --command-config /tmp/client.properties \
  --create --topic perf-test --partitions 3 --replication-factor 3
```

**One node down.** On kafka03 `systemctl stop kafka`, then from kafka01:

```bash
seq 1 1000 | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server $B --command-config /tmp/client.properties --topic perf-test
```

Expected: no error (2 of 3 replicas satisfy `min.insync.replicas=2`).
`systemctl start kafka` on kafka03 and wait until
`--under-replicated-partitions` is empty again.

**Rolling restart under load.** In one terminal:

```bash
/opt/kafka/bin/kafka-producer-perf-test.sh --topic perf-test --num-records 1000000 --throughput 2000 \
  --record-size 512 --command-config /tmp/client.properties --bootstrap-server $B
```

In another, on the control node:
`ansible-playbook -i inventories/test/hosts.yml playbooks/rolling-restart.yml --ask-vault-pass`.
Expected: the playbook ends without failures; the producer's final line
reports all records sent and the output has no `ERROR` lines (retriable
`WARN ... NOT_LEADER_OR_FOLLOWER` is normal). Delete the topic afterwards.

## Monitoring

* `monitoring/vmagent-scrape.yml`: jobs `kafka-jmx` (7071 on brokers),
  `node` (9100, all hosts), `kafka-exporter` (9308), `schema-registry`
  (7071 on kafkasvc01, JMX exporter in the container), `kafka-connect`
  (7072), `kafka-canary` (8082). Targets are TEST; copy for PROD.
* `monitoring/kafka-alerts.yml`: offline partitions, active controller
  count != 1, under-min-ISR, under-replicated > 5 min, fenced brokers,
  exporter / node down, `/data` > 75% / > 85%, growing consumer lag,
  request handler idle < 20%, certificate expiry < 30 / < 7 days
  (textfile metric from `kafka_tls`), Schema Registry down, Kafka Connect
  down / failed connector or task / record errors, canary errors.
  The Kafka metric names were checked against a running 4.3.1 broker
  with the rules from `kafka_jmx_exporter`.
* Kafka authorizer decisions go to `/var/log/kafka/kafka-authorizer.log`
  (denials at INFO) for shipping to the SIEM.

## broker.rack

`kafka_rack` in host_vars defaults to the node name (`kafka01`...). It
must say on which **hypervisor host** the VM runs: Kafka spreads the three
replicas of a partition over different racks, and
`RackAwareReplicaSelector` lets consumers read from a replica in their own
rack. If two VMs share a hypervisor host but have different rack values,
one host failure can take two replicas of a partition and break
`min.insync.replicas=2`. Ask the virtualisation team for the placement
(and anti-affinity) and set the values accordingly; changing a rack later
needs a rolling restart and a partition reassignment for existing topics.

## Checks and molecule

Locally / in CI (`.github/workflows/lint.yml`, `.pre-commit-config.yaml`):

```bash
scripts/check-offline.sh        # no get_url/uri/pip/dnf/package/pull or external URLs in roles/
yamllint -s .
ansible-lint                    # profile production
for p in playbooks/*.yml; do ansible-playbook -i inventories/test/hosts.yml $p --syntax-check; done
```

`molecule/default` brings up three AlmaLinux 10 containers with systemd,
runs `kafka_tls` (self-signed), `kafka_jmx_exporter`, `kafka`,
`kafka_topics`, `kafka_users`, checks idempotence, then verifies a 3/3
quorum, a topic with RF=3, and that an `acks=all` SCRAM producer writes
100 records that a consumer reads back. It needs Docker and internet
(images, Java from the AlmaLinux repos), so it runs on a developer
machine, not on the control node:

```bash
python -m pip install molecule "molecule-plugins[docker]"
scripts/fetch-artifacts.sh
molecule test
```

## Default decisions

Choices not fixed by the task, taken from its defaults or made here:

| Question | Decision |
|---|---|
| Client authentication | SCRAM-SHA-512 |
| Secrets | ansible-vault, referenced from one place for a later switch to HashiCorp Vault |
| Combined mode on PROD | yes, confirmed by the owner; no dedicated controllers |
| OS level (disk, chrony, sysctl, firewalld, SELinux, packages) | by hand, not in Ansible (owner's decision); documented above |
| Services runtime | Docker + compose (Docker from ansible-ib), not Podman |
| Images | pulled from the Nexus docker-proxy, not shipped as archives |
| Schema Registry | `confluentinc/cp-schema-registry:8.3.2` behind a variable |
| Broker principal | certificate CN via `ssl.principal.mapping.rules` |
| Truststore for services | PEM CA file (`ssl.truststore.type=PEM`), no stores on kafkasvc01 |
| Schema Registry metrics | JMX exporter javaagent in the container on 7071 |
| Kafka UI | full rights (owner's decision): not read-only, All ACLs on topics, groups, transactional IDs and the cluster; Schema Registry and Kafka Connect pages enabled |
| Redpanda Console | TEST and PROD (owner's decision), v3.12.0 behind an nginx login form (njs, not HTTP Basic: blocked by the browser policy) on 8090: creates SCRAM users, topics and ACLs (Kafka UI cannot create users); Console's own login is Enterprise-only |
| Kafka Connect | `apache/kafka:4.3.1` image (`connect-distributed.sh`), one worker, JSON converters, env and file config providers for secrets, client overrides allowed so each integration can use its own account |
| Canary ACLs | plus Describe on the cluster (the canary describes it to size its topic) |
| ACL changes | added, never removed automatically |
| Topics | service topics created if missing, never altered |
| Log rotation | daily, 14 days (`kafka_log_retention_days`) |

## Known limitations

* No OS checks: if `/data` is not mounted, Kafka writes to the OS disk.
  Check `findmnt /data` before the first run.
* `--check` cannot simulate a first install (format, start, wait).
* Passwords may not contain `, [ ] " = $ # \` `` ` `` or spaces (SCRAM option
  strings, JAAS strings, env files).
* Removing an ACL or SCRAM account from the inventory does not remove it
  from the cluster.
* Kafka UI with `LOGIN_FORM` is one shared password with full admin rights
  on the cluster. Use OAUTH2/LDAP (and Kafka UI RBAC) on PROD.
* Redpanda Console is the same: one shared password, full admin;
  users, topics and ACLs made there are not in git.
* Kafka Connect: one worker, so a restart of kafkasvc01 stops all
  connectors until it is back. Its REST API (8083) has no
  authentication. Plugins removed from `kafka_connect_plugins` are not
  deleted from the host.
* Docker does not restart a container that only turns unhealthy;
  `restart: unless-stopped` covers crashes.
* The strimzi canary project is archived upstream and its image is only on
  quay.io: the Nexus docker-proxy must proxy quay.io, or disable it with
  `kafka_canary_enabled: false`.
* `kafka-exporter` talks the Kafka 3.9 protocol (`kafka_exporter_kafka_version`);
  re-check after upgrading it.
* Molecule needs Docker with systemd containers and internet; it has not
  been run in the authoring environment. The Kafka roles, the rolling
  restart and the metric names were tested there against a local 3-node
  Kafka 4.3.1 cluster instead, and so was the Kafka Connect worker config
  (SASL password from `${env:}`, a connector with `${file:}` secrets and its
  own account through `consumer.override.*`, the JMX metric names). The
  containers themselves were checked with `docker compose config` only.
