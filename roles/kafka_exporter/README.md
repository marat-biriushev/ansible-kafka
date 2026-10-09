# kafka_exporter

danielqsj/kafka-exporter as a Docker compose project in
`/opt/kafka-exporter/`, metrics on 9308 (consumer group lag for the
`KafkaConsumerLagGrowing` alert). SASL_SSL / SCRAM-SHA-512 with the
`kafka-exporter` account (Describe on topics, groups and the cluster).

The exporter accepts the password only as a command-line flag, so the
container runs `/bin/sh -c 'exec /bin/kafka_exporter ... --sasl.password="$KAFKA_EXPORTER_SASL_PASSWORD"'`
and the value comes from the 0600 `secrets.env`.

`kafka_exporter_kafka_version` is set to 3.9.0: Kafka 4 removed the
request versions older than 2.1 that the exporter's default (2.0.0) uses.

## Variables

All of them are in `defaults/main.yml` and validated by `meta/argument_specs.yml`.

| Variable | Default | Description |
|---|---|---|
| `kafka_exporter_image` | `(see defaults/main.yml)` | Image reference (Nexus mirror). |
| `kafka_exporter_project_dir` | `/opt/kafka-exporter` | Compose project directory. |
| `kafka_exporter_port` | `9308` | Published metrics port. |
| `kafka_exporter_bootstrap_servers` | `{{ kafka_bootstrap_servers }}` | Broker host:port list (CLIENT listener). |
| `kafka_exporter_kafka_user` | `kafka-exporter` | SCRAM account. |
| `kafka_exporter_kafka_password` | `""` | Its password (vault). |
| `kafka_exporter_ca_file` | `/etc/kafka-services/ca.pem` | CA file on the host, mounted read-only. |
| `kafka_exporter_kafka_version` | `3.9.0` | Protocol version for sarama; Kafka 4 needs 2.1+. |
| `kafka_exporter_args_extra` | `[]` | Extra command-line flags. |
| `kafka_exporter_healthcheck` | `(see defaults/main.yml)` | Docker healthcheck test. |
