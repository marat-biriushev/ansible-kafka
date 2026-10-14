# redpanda_console

Redpanda Console as a Docker compose project in `/opt/redpanda-console/`,
behind nginx with a password, port 8090. Kafka over SASL_SSL /
SCRAM-SHA-512 with the `redpanda-console` account. Off by default
(`redpanda_console_enabled`); enabled on TEST.

Why it is here next to Kafka UI: Console can create **SCRAM users with
passwords**, topics and ACLs, so a developer can set up everything an
application needs in one place. Kafka UI cannot create users. Checked
against Apache Kafka 4.3.1 with Console v3.12.0 (Community license):
users, topics and ACLs created through Console work for clients.

Console's own login is a Redpanda Enterprise feature (Console refuses to
start with `authentication.basic` and no license), so nginx in front of it
asks for the password. Console publishes no port; only nginx does.

* Whoever knows the password is a **cluster admin**: the
  `redpanda-console` account has All on topics, groups, transactional IDs
  and the cluster (creating SCRAM users and ACLs is a cluster Alter).
* One shared password, no per-person rights. Users, topics and ACLs made
  in Console are not in git, so this is for TEST, not PROD.
* Telemetry to Redpanda is off (`analytics.enabled: false`).

Files: `config.yml` (Console, no secrets), `secrets.env` (0600,
`KAFKA_SASL_PASSWORD`), `nginx.conf`, `htpasswd` (SHA-512 crypt with a
salt fixed per cluster, so reruns do not change it). A change to any of
the bind-mounted files restarts the containers.

## Variables

All of them are in `defaults/main.yml` and validated by `meta/argument_specs.yml`.

| Variable | Default | Description |
|---|---|---|
| `redpanda_console_enabled` | `false` | Deploy Redpanda Console on this host. |
| `redpanda_console_image` | `(see defaults/main.yml)` | Console image reference (Nexus mirror). |
| `redpanda_console_nginx_image` | `(see defaults/main.yml)` | nginx image reference (Nexus mirror). |
| `redpanda_console_project_dir` | `/opt/redpanda-console` | Compose project directory. |
| `redpanda_console_port` | `8090` | Port nginx publishes on the host. |
| `redpanda_console_bootstrap_servers` | `{{ kafka_bootstrap_servers }}` | Broker host:port list (CLIENT listener). |
| `redpanda_console_kafka_user` | `redpanda-console` | SCRAM account of Console (All ACLs). |
| `redpanda_console_kafka_password` | `""` | Its password (vault). |
| `redpanda_console_ca_file` | `/etc/kafka-services/ca.pem` | CA file on the host, mounted read-only. |
| `redpanda_console_schema_registry_url` | `""` | Schema Registry URL; empty hides it. |
| `redpanda_console_kafka_connect_url` | `""` | Kafka Connect REST URL; empty hides the Connect pages. |
| `redpanda_console_login_user` | `admin` | nginx basic auth user. |
| `redpanda_console_login_password` | `""` | nginx basic auth password (vault). |
| `redpanda_console_htpasswd_salt` | `(see defaults/main.yml)` | Salt of the htpasswd hash, fixed per cluster. |
