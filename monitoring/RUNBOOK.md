# Kafka runbook

One section per alert in `kafka-alerts.yml`; each alert's `runbook_url`
points at its heading. Publish this page in Confluence and replace
`RUNBOOK_BASE_URL` in the rules with its address:

```bash
sed -i 's#RUNBOOK_BASE_URL#https://space.ipotekabank.uz/<page>#' monitoring/kafka-alerts.yml
```

(Confluence makes its own anchors from headings; check one link after
publishing.)

Shell setup on any broker, used below:

```bash
K=/opt/kafka/bin; C=/etc/kafka/admin.properties; B=$(hostname -f):9091
```

Common checks: `systemctl status kafka`, `journalctl -u kafka -n 100`,
`/var/log/kafka/server.log`, `df -h /data`.

## KafkaOfflinePartitions

**Meaning:** partitions have no leader; their producers and consumers are down.
Usually two or more brokers are down at once (RF 3, min ISR 2).

**Check:**
```bash
$K/kafka-topics.sh --bootstrap-server $B --command-config $C --describe --unavailable-partitions
$K/kafka-metadata-quorum.sh --bootstrap-server $B --command-config $C describe --replication
```
**Act:** start the stopped brokers (`systemctl start kafka`), one at a time.
Unclean leader election is off on purpose: do not turn it on, it loses data.

## KafkaActiveControllerCountNotOne

**Meaning:** 0 = no controller leader (quorum lost: 2 of 3 nodes down);
2+ = split-brain (should not happen with KRaft; check clocks and network).

**Check:** `kafka-metadata-quorum.sh ... describe --status` (LeaderId, voters),
`chronyc tracking` on all nodes.
**Act:** bring the nodes back; a quorum needs 2 of 3 controllers.

## KafkaUnderMinIsrPartitions

**Meaning:** fewer than 2 in-sync replicas; producers with `acks=all` get
`NOT_ENOUGH_REPLICAS`.

**Check:** `kafka-topics.sh ... --describe --under-min-isr-partitions`; which
broker is missing from the ISR.
**Act:** fix or restart that broker; see KafkaBrokerDown.

## KafkaUnderReplicatedPartitions

**Meaning:** some replicas lag behind. A minute or two during a restart or
rolling restart is normal (warning); 5 minutes and more is critical.

**Check:** `kafka-topics.sh ... --describe --under-replicated-partitions`, the
broker missing from the ISR, its disk, network and GC (`server.log`).
**Act:** a broker that was down catches up by itself; if it does not, look at
its disk (`df`, `iostat`), network and `RequestHandlerAvgIdlePercent`.

## KafkaIsrFlapping

**Meaning:** replicas keep leaving and rejoining the ISR: unstable network or
an overloaded broker (GC pauses, slow disk).

**Check:** which node; its `server.log` ("Shrinking ISR"), GC, network
errors between brokers, disk latency.
**Act:** remove the cause; `replica.lag.time.max.ms` is not the fix.

## KafkaFencedBrokers

**Meaning:** a broker lost its registration with the controller (heartbeats
missed: long GC, network, overloaded).

**Check:** `kafka-metadata-quorum.sh ... describe --replication`, `server.log`
on the fenced broker.
**Act:** usually it unfences itself; if not, restart it (`systemctl restart kafka`).

## KafkaBrokerDown

**Meaning:** the JMX exporter (inside the Kafka process, port 7071) does not
answer: Kafka is down, or 7071 is blocked from the monitoring host.

**Check:** `systemctl status kafka`, `journalctl -u kafka -n 100`,
`curl -s localhost:7071/metrics | head` on the node.
**Act:** start it; if it fails to start, read `server.log` (disk full,
`meta.properties` mismatch, certificate problems). One node down: the cluster
keeps working, fix it without hurry but today.

## KafkaRequestHandlerIdleLow

**Meaning:** request handler threads are busy: below 40% idle a warning, below
20% critical (the broker is overloaded and latency grows).

**Check:** request rate per type, which clients/topics grew, disk and CPU.
**Act:** find the client that grew (Kafka UI, `kafka_server_brokertopicmetrics_*`);
add quotas or capacity.

## KafkaRequestLatencyHigh

**Meaning:** broker-side p99 for Produce or FetchConsumer is above twice its
7-day average.

**Check:** the same node's disk, GC, network, `RequestHandlerAvgIdlePercent`,
under-replicated partitions.
**Act:** as for KafkaRequestHandlerIdleLow; after a planned load change,
the baseline follows within days.

## KafkaProduceErrors

**Meaning:** under 99.9% of produce requests succeed over 5 minutes.

**Check:** under-min-ISR and offline partitions, authorization errors in
`/var/log/kafka/kafka-authorizer.log`, message size errors in `server.log`.
**Act:** fix the cluster side, or tell the client team (ACL, message too large).

## KafkaNodeDown

**Meaning:** node_exporter (9100) does not answer: the host is down, or 9100
is blocked.

**Check:** ping/SSH the host; `systemctl status node_exporter`.
**Act:** bring the host back; for a broker also see KafkaBrokerDown.

## KafkaDataDiskUsage

**Meaning:** `/data` above 75% (warning) or 85% (critical). At 100% the broker
stops.

**Check:** `df -h /data`, the largest topics (Kafka UI or
`kafka_log_log_size_bytes`).
**Act:** lower `retention.ms`/`retention.bytes` on the large topics, delete
test topics, or grow the disk. Never delete files in `/data/kafka` by hand.

## KafkaCertificateExpires

**Meaning:** a broker certificate expires in under 30 days (warning) or 7 days
(critical). Expired certificates stop inter-broker traffic and clients.

**Act:** self-signed: rerun `playbooks/kafka.yml` (certificates within 30 days
of expiry are reissued and the brokers restarted one by one). Bank CA: put the
new files into `certs/<env>/` and run `playbooks/kafka.yml`.

## KafkaConsumerLag

**Meaning:** a consumer group keeps falling behind for 15 minutes (critical),
or its lag is above the threshold (warning; one default for all groups,
key groups get their own once their SLO is agreed).

**Check:** the group in Kafka UI: members present? committing offsets?
**Act:** tell the owning team; their consumer is stopped, failing on a message,
or too slow (more partitions/consumers).

## KafkaExporterDown

**Meaning:** kafka-exporter on kafkasvc01 (9308) does not answer; consumer lag
is not monitored.

**Check/Act:** `docker ps` / `docker logs kafka-exporter` on kafkasvc01;
`docker compose --project-directory /opt/kafka-exporter up -d`.

## SchemaRegistryDown

**Meaning:** Schema Registry (8081, metrics 7071) on kafkasvc01 does not
answer; producers/consumers with Avro/Protobuf schemas fail.

**Check/Act:** `docker logs schema-registry --tail 100`;
`docker compose --project-directory /opt/schema-registry up -d`.

## KafkaConnectDown

**Meaning:** the Kafka Connect worker (8083, metrics 7072) does not answer;
all connectors are stopped.

**Check/Act:** `docker logs kafka-connect --tail 100`;
`docker compose --project-directory /opt/kafka-connect up -d`. Connectors
resume from their stored offsets.

## KafkaConnectFailed

**Meaning:** a connector or one of its tasks is FAILED.

**Check:** `curl -s localhost:8083/connectors/<name>/status` on kafkasvc01 (the
trace says why: database unreachable, credentials, schema change).
**Act:** fix the cause, then
`curl -X POST 'localhost:8083/connectors/<name>/restart?includeTasks=true&onlyFailed=true'`.

## KafkaConnectRecordErrors

**Meaning:** a task skipped or failed records in the last 10 minutes
(conversion or transform errors).

**Check:** the connector's error settings (`errors.tolerance`, dead letter
queue topic) and its logs.
**Act:** tell the owning team; fix the data or the connector config.
