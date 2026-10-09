# kafka_canary

Strimzi canary as a Docker compose project in `/opt/kafka-canary/`,
published on 8082 (container port 8080, `/metrics`, `/liveness`). It
produces and consumes on `__strimzi_canary` with the `canary` account and
group, and exports latency and error counters
(`strimzi_canary_records_produced_failed_total`,
`strimzi_canary_consumer_error_total`, ...). Switched off with
`kafka_canary_enabled: false`.

Two caveats:

* The image is published only on quay.io. The Docker Hub proxy in Nexus
  does not have it; set `kafka_canary_image` to a Nexus repository that
  proxies quay.io.
* The upstream project is archived (Strimzi proposal 086). It works, but
  gets no fixes; check that the pinned tag exists in your mirror.

TLS uses the mounted CA chain through `SSL_CERT_FILE`.

## Variables

All of them are in `defaults/main.yml` and validated by `meta/argument_specs.yml`.

| Variable | Default | Description |
|---|---|---|
| `kafka_canary_enabled` | `true` | Deploy the canary. |
| `kafka_canary_image` | `quay.io/strimzi/canary:0.11.0` | Image reference; quay.io only, see the role README. |
| `kafka_canary_project_dir` | `/opt/kafka-canary` | Compose project directory. |
| `kafka_canary_port` | `8082` | Published port (container 8080). |
| `kafka_canary_bootstrap_servers` | `{{ kafka_bootstrap_servers }}` | Broker host:port list (CLIENT listener). |
| `kafka_canary_kafka_user` | `canary` | SCRAM account. |
| `kafka_canary_kafka_password` | `""` | Its password (vault). |
| `kafka_canary_ca_file` | `/etc/kafka-services/ca.pem` | CA file on the host, mounted read-only. |
| `kafka_canary_topic` | `__strimzi_canary` | Canary topic. |
| `kafka_canary_group_id` | `canary` | Canary consumer group. |
| `kafka_canary_expected_cluster_size` | `{{ groups['kafka'] \| default([]) \| length }}` | Number of brokers the canary waits for. |
| `kafka_canary_env_extra` | `{}` | Extra environment variables. |
| `kafka_canary_healthcheck` | `["CMD", "curl", "-fsS", "http://localhost:8080/liveness"]` | Docker healthcheck test. |
