# kafka

Installs Apache Kafka from the offline tarball and runs it as a KRaft
combined node (`process.roles=broker,controller`) under systemd. Nothing
else: TLS stores come from `kafka_tls`, the javaagent from
`kafka_jmx_exporter`, accounts from `kafka_users`. The OS (data disk,
sysctl, firewall, Java) is prepared by hand, see the main README.

## What it does

1. Asserts that `kafka_node_id`, `kafka_cluster_id`, `kafka_directory_id`
   and the vault secrets are set, and that IDs are unique in the cluster.
2. Checks on the control node that `artifacts/kafka_2.13-<version>.tgz`
   exists and matches the sha512 in `artifacts.yml`.
3. Creates the `kafka` user, `/etc/kafka`, `/data/kafka`, `/var/log/kafka`;
   unpacks to `/opt/kafka_2.13-<version>` and points `/opt/kafka` at it.
4. Templates `server.properties`, `log4j2.yaml`, `kafka.env` (JVM options,
   javaagent), `admin.properties` (mTLS client for the admin tools) and
   `kafka.service` (`LimitNOFILE`, `TimeoutStopSec=300`).
5. Formats the storage **only if** `/data/kafka/meta.properties` is missing,
   with the same `--initial-controllers` string on every node and
   `--add-scram` for `admin`. If the file exists and its cluster, node or
   directory ID differs from the inventory, the role fails. There is no
   reformat option.
6. Starts all nodes in the same task (the quorum needs them together),
   then waits for 3 voters in `kafka-metadata-quorum describe --status` and
   for `kafka-broker-api-versions` to answer on every node.

## Config changes and restarts

Changing a template never restarts a running broker directly. The handler
`Kafka restart required` only sets the fact `kafka_restart_required` (and
only when Kafka was running before the run). `playbooks/kafka.yml` ends by
importing `playbooks/rolling-restart.yml` with `kafka_rr_only_required:
true`, which restarts just those nodes, one at a time, active controller
last.

## Listeners

| Name | Port | Protocol | Use |
|---|---|---|---|
| CLIENT | 9092 | SASL_SSL, SCRAM-SHA-512 | clients and services |
| INTERNAL | 9091 | SSL, `ssl.client.auth=required` | replication, Ansible admin tools |
| CONTROLLER | 9093 | SSL, `ssl.client.auth=required` | KRaft quorum |

`advertised.listeners` use the node FQDN. Certificate principals are mapped
to the CN (`ssl.principal.mapping.rules`), so `super.users` lists brokers
by FQDN plus `User:admin`.

## Variables

All of them are in `defaults/main.yml` and validated by `meta/argument_specs.yml`.

| Variable | Default | Description |
|---|---|---|
| `kafka_version` | `4.3.1` | Kafka release; must match the kafka entry in artifacts.yml. |
| `kafka_scala_version` | `2.13` | Scala build of the tarball. |
| `kafka_tarball` | `kafka_{{ kafka_scala_version }}-{{ kafka_version }}.tgz` | Tarball file name in artifacts/. |
| `kafka_artifacts_dir` | `(see defaults/main.yml)` | Directory with the offline artifacts on the control node (global artifacts_dir if set). |
| `kafka_artifacts_manifest` | `(see defaults/main.yml)` | Path of artifacts.yml on the control node (global artifacts_manifest if set). |
| `kafka_user` | `kafka` | Service account Kafka runs as. |
| `kafka_group` | `kafka` | Group of the service account. |
| `kafka_install_root` | `/opt` | Where versioned Kafka directories are unpacked. |
| `kafka_home` | `/opt/kafka` | Symlink to the active version; used by the unit and the admin tools. |
| `kafka_install_dir` | `(see defaults/main.yml)` | Versioned install directory. |
| `kafka_conf_dir` | `/etc/kafka` | Configuration directory (server.properties, log4j2.yaml, kafka.env, admin.properties). |
| `kafka_data_dir` | `/data/kafka` | log.dirs on the data disk; meta.properties lives here. |
| `kafka_log_dir` | `/var/log/kafka` | Application logs (server.log, kafka-authorizer.log, ...). |
| `kafka_node_id` | `""` | node.id, unique per node (host_vars). Required. |
| `kafka_cluster_id` | `""` | Cluster UUID from kafka-storage.sh random-uuid (group_vars). Required, never changes. |
| `kafka_directory_id` | `""` | Metadata log directory UUID of this node (host_vars). Required, never changes. |
| `kafka_rack` | `{{ inventory_hostname \| regex_replace('^.*-', '') }}` | broker.rack; must name the hypervisor host the VM runs on. |
| `kafka_domain` | `""` | DNS domain appended to inventory hostnames. |
| `kafka_fqdn` | `{{ inventory_hostname ~ kafka_domain_suffix }}` | FQDN of this node, used in advertised.listeners and certificates. |
| `kafka_domain_suffix` | `{{ ('.' ~ kafka_domain) if kafka_domain else '' }}` | Internal helper ("." + kafka_domain, or empty). |
| `kafka_cluster_group` | `kafka` | Inventory group whose hosts form the quorum. |
| `kafka_cluster_fqdns` | `(see defaults/main.yml)` | FQDNs of all quorum members (derived). |
| `kafka_listen_address` | `""` | Bind address of all listeners; empty means all interfaces. |
| `kafka_client_port` | `9092` | CLIENT listener (SASL_SSL, SCRAM-SHA-512). |
| `kafka_internal_port` | `9091` | INTERNAL listener (mTLS), inter-broker and Ansible admin traffic. |
| `kafka_controller_port` | `9093` | CONTROLLER listener (mTLS), KRaft quorum. |
| `kafka_heap` | `4g` | JVM heap (-Xms = -Xmx). TEST 4g, PROD 6g. |
| `kafka_jvm_performance_opts` | `(see defaults/main.yml)` | GC and JVM flags (KAFKA_JVM_PERFORMANCE_OPTS). |
| `kafka_opts_extra` | `""` | Extra KAFKA_OPTS appended after the javaagent. |
| `kafka_limit_nofile` | `100000` | LimitNOFILE of kafka.service. |
| `kafka_timeout_stop_sec` | `300` | TimeoutStopSec; time for a controlled shutdown. |
| `kafka_jmx_exporter_enabled` | `true` | Load the JMX exporter javaagent (kafka role) / its port and paths (both roles must agree). |
| `kafka_jmx_exporter_port` | `7071` | Port of the JMX exporter HTTP endpoint. |
| `kafka_jmx_exporter_jar` | `/opt/jmx_exporter/jmx_prometheus_javaagent.jar` | Path of the javaagent jar on the broker. |
| `kafka_jmx_exporter_config` | `/etc/kafka/jmx-exporter.yml` | Path of the exporter rules on the broker. |
| `kafka_tls_keystore` | `/etc/kafka/tls/keystore.p12` | PKCS12 keystore built by kafka_tls. |
| `kafka_tls_truststore` | `/etc/kafka/tls/truststore.p12` | PKCS12 truststore built by kafka_tls. |
| `kafka_tls_keystore_password` | `""` | Keystore and key password (vault). |
| `kafka_tls_truststore_password` | `""` | Truststore password (vault). |
| `kafka_tls_protocols` | `TLSv1.3,TLSv1.2` | ssl.enabled.protocols. |
| `kafka_admin_user` | `admin` | SCRAM super user created at format time. |
| `kafka_admin_password` | `""` | Its password (vault); 12+ characters, none of , [ ] " = or spaces. |
| `kafka_super_users` | `(see defaults/main.yml)` | super.users; broker certificate CNs plus User:admin (derived). |
| `kafka_default_replication_factor` | `3` | default.replication.factor. |
| `kafka_min_insync_replicas` | `2` | min.insync.replicas. |
| `kafka_num_partitions` | `3` | num.partitions for topics created without an explicit count. |
| `kafka_log_retention_hours` | `168` | log.retention.hours. |
| `kafka_num_network_threads` | `3` | num.network.threads. |
| `kafka_num_io_threads` | `8` | num.io.threads. |
| `kafka_server_properties_extra` | `{}` | Extra server.properties entries, key -> value. |
| `kafka_start_retries` | `60` | Attempts while waiting for the quorum and the brokers after start. |
| `kafka_start_delay` | `5` | Seconds between those attempts. |
| `kafka_log_level` | `INFO` | Root and kafka logger level. |
| `kafka_log_retention_days` | `14` | Days rolled log files are kept. |
| `kafka_log4j_appenders` | `(see defaults/main.yml)` | Appender name -> log file base name. |
