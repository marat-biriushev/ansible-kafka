# kafka_jmx_exporter

Puts `jmx_prometheus_javaagent` (from `artifacts/`, sha256 checked against
`artifacts.yml`) into `/opt/jmx_exporter/` and its rules into
`/etc/kafka/jmx-exporter.yml`. The `kafka` role loads it with
`-javaagent:<jar>=7071:<rules>` in `KAFKA_OPTS`; keep the path and port
variables of both roles equal (the defaults are).

The rules export `kafka.server` (including `type=raft-metrics`),
`kafka.controller`, `kafka.network`, `kafka.log` and JVM memory/GC. The
metric names used by `monitoring/kafka-alerts.yml` were checked against a
running Kafka 4.3.1 with this exact rules file:

| Metric | Source MBean |
|---|---|
| `kafka_controller_kafkacontroller_offlinepartitionscount` | `kafka.controller:type=KafkaController,name=OfflinePartitionsCount` |
| `kafka_controller_kafkacontroller_activecontrollercount` | `...,name=ActiveControllerCount` |
| `kafka_controller_kafkacontroller_fencedbrokercount` | `...,name=FencedBrokerCount` |
| `kafka_server_replicamanager_underreplicatedpartitions` | `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` |
| `kafka_server_replicamanager_underminisrpartitioncount` | `...,name=UnderMinIsrPartitionCount` |
| `kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent` | `kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent` (OneMinuteRate, 0..1) |
| `kafka_server_raft_metrics_*` | `kafka.server:type=raft-metrics` |

A change of the jar or the rules marks the broker for a rolling restart.

## Variables

All of them are in `defaults/main.yml` and validated by `meta/argument_specs.yml`.

| Variable | Default | Description |
|---|---|---|
| `kafka_jmx_exporter_version` | `1.7.0` | jmx_prometheus_javaagent version; must match artifacts.yml. |
| `kafka_jmx_exporter_file` | `(see defaults/main.yml)` | Jar file name in artifacts/. |
| `kafka_jmx_exporter_artifacts_dir` | `(see defaults/main.yml)` | Directory with the offline artifacts on the control node. |
| `kafka_jmx_exporter_artifacts_manifest` | `(see defaults/main.yml)` | Path of artifacts.yml on the control node. |
| `kafka_jmx_exporter_dir` | `/opt/jmx_exporter` | Directory for the jar on the broker. |
| `kafka_jmx_exporter_jar` | `{{ kafka_jmx_exporter_dir }}/jmx_prometheus_javaagent.jar` | Path of the javaagent jar on the broker. |
| `kafka_jmx_exporter_config` | `/etc/kafka/jmx-exporter.yml` | Path of the exporter rules on the broker. |
| `kafka_jmx_exporter_port` | `7071` | Port of the JMX exporter HTTP endpoint. |
| `kafka_jmx_exporter_group` | `kafka` | Group that may read the rules file (Kafka's group). |
