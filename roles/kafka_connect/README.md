# kafka_connect

Kafka Connect in distributed mode as a Docker compose project in
`/opt/kafka-connect/`, REST API on 8083, JMX exporter on 7072. The image is
`apache/kafka` of the broker version (Apache license), started with
`connect-distributed.sh`. One worker on the services VM; more workers with
the same `kafka_connect_group_id` can join later.

* **Kafka.** SASL_SSL / SCRAM-SHA-512 as `kafka-connect`, for the worker
  and for the connectors' producers, consumers and admin clients. The
  worker config holds no secret: the password is read with
  `${env:KAFKA_CONNECT_SASL_PASSWORD}` (EnvVarConfigProvider) from the 0600
  `secrets.env`.
* **Internal topics.** `connect-configs` (1 partition), `connect-offsets`
  (25), `connect-status` (5), compacted, RF=3; created by `kafka_topics`
  from group_vars. The worker has All on `connect-*` topics and groups
  (sink connector groups are `connect-<name>`), Describe/DescribeConfigs
  on the cluster.
* **Connector topics.** Not covered by the worker ACLs. Give each
  integration its own SCRAM account and set it in the connector with
  `producer.override.sasl.jaas.config` / `consumer.override.sasl.jaas.config`
  (`kafka_connect_client_override_policy: All` allows this), or grant
  the topics to `User:kafka-connect`.
* **Plugins.** None are bundled except the ones in Apache Kafka (file,
  MirrorMaker). Database connectors (Debezium, JDBC...) are downloaded
  into `artifacts/connect-plugins/` (pinned with a sha256 in the
  `connect_plugins` list of `artifacts.yml`), listed in
  `kafka_connect_plugins`, and unpacked to `plugins/<name>/`. A new archive
  replaces the whole plugin directory; a plugin removed from the list stays
  until deleted by hand.
* **Connector secrets.** Database passwords go into
  `kafka_connect_connector_secrets` (vault) and end up in
  `secrets/connectors.properties` (0600, container user). A connector
  refers to them as
  `${file:/etc/kafka-connect/secrets/connectors.properties:<key>}`; the
  `connect-configs` topic and the REST API then show the placeholder,
  never the password.
* **Converters.** JSON (`JsonConverter`) by default. Avro / JSON Schema
  with Schema Registry need the Confluent converter jars as a plugin.

Connectors themselves are created through the REST API or Kafka UI, not
by this role.

Example (PostgreSQL CDC with Debezium, once the plugin is installed):

```bash
curl -X PUT -H 'Content-Type: application/json' \
  http://int-res-test-kafkasvc01.ipa.ipotekabank.uz:8083/connectors/crm-cdc/config -d '{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "database.hostname": "crm-db.ipa.ipotekabank.uz",
  "database.user": "debezium",
  "database.password": "${file:/etc/kafka-connect/secrets/connectors.properties:crm_db_password}",
  "database.dbname": "crm",
  "topic.prefix": "crm.cdc",
  "producer.override.sasl.jaas.config": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"crm\" password=\"${file:/etc/kafka-connect/secrets/connectors.properties:crm_kafka_password}\";"
}'
```

The REST API has no authentication: restrict 8083 with the firewall to
Kafka UI and the operators.

## Variables

All of them are in `defaults/main.yml` and validated by `meta/argument_specs.yml`.

| Variable | Default | Description |
|---|---|---|
| `kafka_connect_image` | `(see defaults/main.yml)` | Image reference (Nexus mirror); apache/kafka of the broker version. |
| `kafka_connect_project_dir` | `/opt/kafka-connect` | Compose project directory. |
| `kafka_connect_port` | `8083` | Published REST port. |
| `kafka_connect_advertised_host` | `(see defaults/main.yml)` | rest.advertised.host.name, the host FQDN. |
| `kafka_connect_container_uid` | `1000` | User id of appuser in the image; owns the connector secrets file. |
| `kafka_connect_bootstrap_servers` | `{{ kafka_bootstrap_servers }}` | Broker host:port list (CLIENT listener). |
| `kafka_connect_kafka_user` | `kafka-connect` | SCRAM account of the worker. |
| `kafka_connect_kafka_password` | `""` | Its password (vault). |
| `kafka_connect_ca_file` | `/etc/kafka-services/ca.pem` | CA file on the host, mounted read-only. |
| `kafka_connect_group_id` | `connect-cluster` | Worker group id. |
| `kafka_connect_config_topic` | `connect-configs` | Internal topic for connector configs. |
| `kafka_connect_offset_topic` | `connect-offsets` | Internal topic for source offsets. |
| `kafka_connect_status_topic` | `connect-status` | Internal topic for statuses. |
| `kafka_connect_client_override_policy` | `All` | connector.client.config.override.policy (All, Principal, None). |
| `kafka_connect_heap` | `1g` | JVM heap. |
| `kafka_connect_plugins` | `[]` | Plugins to install, [{name}], each pinned in connect_plugins of artifacts.yml. |
| `kafka_connect_artifacts_dir` | `(see defaults/main.yml)` | Directory with the offline artifacts on the control node. |
| `kafka_connect_artifacts_manifest` | `(see defaults/main.yml)` | Path of artifacts.yml on the control node. |
| `kafka_connect_connector_secrets` | `{}` | Secrets for connector configs, key -> value (vault); read through the file config provider. |
| `kafka_connect_jmx_exporter_enabled` | `true` | Run the JMX exporter javaagent in the container. |
| `kafka_connect_jmx_exporter_port` | `7072` | Published JMX exporter port. |
| `kafka_connect_jmx_exporter_file` | `jmx_prometheus_javaagent-1.7.0.jar` | Jar file name in artifacts/. |
| `kafka_connect_properties_extra` | `{}` | Extra worker properties, key -> value. |
| `kafka_connect_healthcheck` | `(see defaults/main.yml)` | Docker healthcheck test. |
