# kafka_topics

Creates the topics the services need before they start (`_schemas` for
Schema Registry: 1 partition, RF=3, `cleanup.policy=compact`,
`min.insync.replicas=2`). Missing topics are created; existing ones are
not altered. Runs on one broker over the INTERNAL listener. Application
topics are out of scope.

## Variables

All of them are in `defaults/main.yml` and validated by `meta/argument_specs.yml`.

| Variable | Default | Description |
|---|---|---|
| `kafka_topics_list` | `[]` | Topics to create if missing, [{name, partitions, replication_factor, config}]. |
| `kafka_topics_kafka_home` | `/opt/kafka` | Kafka home on the broker that runs the admin tools. |
| `kafka_topics_admin_config` | `/etc/kafka/admin.properties` | admin.properties (mTLS) for the admin tools. |
| `kafka_topics_bootstrap_server` | `(see defaults/main.yml)` | INTERNAL listener of this broker. |
