# schema_registry

Confluent Schema Registry as a Docker compose project in
`/opt/schema-registry/`. Docker and the compose plugin are installed
beforehand with the `docker` role from ansible-ib; the image comes from the
Nexus docker-proxy (`container_registry`).

* Kafka over the CLIENT listener, SASL_SSL / SCRAM-SHA-512, account
  `schema-registry`; the CA chain from `/etc/kafka-services/ca.pem` is
  mounted read-only and used as a PEM truststore.
* The JAAS line with the password is in `secrets.env` (root, 0600), read by
  compose through `env_file`; `compose.yml` holds no secrets.
* The JMX exporter javaagent (same jar as on the brokers, from
  `artifacts/`) runs in the container and is published on 7071 for the
  `schema-registry` scrape job.
* `restart: unless-stopped` and a healthcheck (`curl /subjects`). Docker
  marks an unhealthy container but does not restart it by itself.

The license of `confluentinc/cp-schema-registry` is an open question;
`schema_registry_image` can point at Karapace or Apicurio instead (their
environment variables differ, adjust `compose.yml.j2`).

## Variables

All of them are in `defaults/main.yml` and validated by `meta/argument_specs.yml`.

| Variable | Default | Description |
|---|---|---|
| `schema_registry_image` | `(see defaults/main.yml)` | Image reference (Nexus mirror). |
| `schema_registry_project_dir` | `/opt/schema-registry` | Compose project directory. |
| `schema_registry_port` | `8081` | Published REST port. |
| `schema_registry_host_name` | `(see defaults/main.yml)` | SCHEMA_REGISTRY_HOST_NAME. |
| `schema_registry_bootstrap_servers` | `{{ kafka_bootstrap_servers }}` | Broker host:port list (CLIENT listener). |
| `schema_registry_kafka_user` | `schema-registry` | SCRAM account. |
| `schema_registry_kafka_password` | `""` | Its password (vault). |
| `schema_registry_ca_file` | `/etc/kafka-services/ca.pem` | CA file on the host, mounted read-only. |
| `schema_registry_topic` | `_schemas` | Storage topic. |
| `schema_registry_group_id` | `schema-registry` | Group used for leader election. |
| `schema_registry_jmx_exporter_enabled` | `true` | Run the JMX exporter javaagent in the container. |
| `schema_registry_jmx_exporter_port` | `7071` | Published JMX exporter port. |
| `schema_registry_jmx_exporter_file` | `jmx_prometheus_javaagent-1.7.0.jar` | Jar file name in artifacts/. |
| `schema_registry_artifacts_dir` | `(see defaults/main.yml)` | Directory with the offline artifacts on the control node. |
| `schema_registry_heap` | `512m` | JVM heap. |
| `schema_registry_healthcheck` | `["CMD", "curl", "-fsS", "http://localhost:8081/subjects"]` | Docker healthcheck test. |
| `schema_registry_env_extra` | `{}` | Extra environment variables. |
