"""Generate monitoring/grafana/kafka-integration-resilience.json:

    python3 monitoring/grafana/gen_dashboard.py monitoring/grafana/kafka-integration-resilience.json \
        monitoring/grafana/kafka-integration-resilience.resource.json
"""
import json
import sys

DS = {"type": "prometheus", "uid": "${datasource}"}
J = 'job=~"int-res-.+-kafka-jmx", cluster="$cluster"'
JN = J + ', node=~"$node"'
NODE = 'job=~"int-res-.+-kafka-node", cluster="$cluster"'
LAG = 'job=~"int-res-.+-kafka-lag", cluster="$cluster"'
CON = 'job=~"int-res-.+-kafka-connect", cluster="$cluster"'

panels = []
pid = [0]
y = [0]


def nid():
    pid[0] += 1
    return pid[0]


def row(title, collapsed=False):
    panels.append({"type": "row", "title": title, "id": nid(), "collapsed": collapsed,
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": y[0]}, "panels": []})
    y[0] += 1


def target(expr, legend="", instant=False, fmt=None):
    t = {"datasource": DS, "expr": expr, "legendFormat": legend, "refId": "A", "range": not instant, "instant": instant}
    if fmt:
        t["format"] = fmt
    return t


def place(p, w, h, x):
    p["gridPos"] = {"h": h, "w": w, "x": x, "y": y[0]}
    p["id"] = nid()
    p["datasource"] = DS
    panels.append(p)


def stats(items, h=4):
    """items: (title, expr, unit, thresholds[(color, value)], description)"""
    w = 24 // len(items)
    for i, (title, expr, unit, steps, desc) in enumerate(items):
        place({
            "type": "stat", "title": title, "description": desc,
            "targets": [target(expr, instant=True)],
            "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background",
                        "graphMode": "none", "textMode": "value"},
            "fieldConfig": {"defaults": {"unit": unit, "noValue": "0",
                                         "thresholds": {"mode": "absolute", "steps": [{"color": c, "value": v} for c, v in steps]}},
                            "overrides": []},
        }, w, h, i * w)
    y[0] += h


def ts(title, targets, unit="short", w=12, x=0, desc="", stack=False, min0=True):
    place({
        "type": "timeseries", "title": title, "description": desc,
        "targets": [dict(t, refId=chr(65 + i)) for i, t in enumerate(targets)],
        "fieldConfig": {"defaults": dict({"unit": unit,
                                          "custom": {"lineWidth": 1, "fillOpacity": 10,
                                                     "stacking": {"mode": "normal" if stack else "none"}}},
                                         **({"min": 0} if min0 else {})),
                        "overrides": []},
        "options": {"legend": {"displayMode": "table", "placement": "bottom", "calcs": ["lastNotNull", "max"]},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
    }, w, 8, x)


def table(title, expr, w=12, x=0, desc="", h=8, value_name="Value", unit="short", mappings=None):
    place({
        "type": "table", "title": title, "description": desc,
        "targets": [target(expr, instant=True, fmt="table")],
        "transformations": [{"id": "organize", "options": {"excludeByName": {"Time": True, "__name__": True, "instance": True, "job": True, "cluster": True, "node": True}, "renameByName": {"Value": value_name}}}],
        "fieldConfig": {"defaults": {"unit": unit, "mappings": mappings or []}, "overrides": []},
        "options": {"showHeader": True, "sortBy": [{"displayName": value_name, "desc": True}]},
    }, w, h, x)


RED0 = [("green", None), ("red", 1)]
# Same as the kafka:produce_success_ratio:rate5m recording rule, inline so the
# dashboard works whether or not kafka-alerts.yml is loaded
PRODUCE_OK = (f'1 - sum(rate(kafka_server_brokertopicmetrics_failedproducerequests_total{{{J}, topic=""}}[5m]))'
              f' / clamp_min(sum(rate(kafka_server_brokertopicmetrics_totalproducerequests_total{{{J}, topic=""}}[5m])), 1e-9)')
LAG_BY = f'sum by (consumergroup, topic) (kafka_consumergroup_lag{{{LAG}, consumergroup=~"$consumergroup"}})'
ONE = [("red", None), ("green", 1), ("red", 2)]

# --- Overview ---------------------------------------------------------------
row("Cluster overview")
stats([
    ("Brokers up", f'sum(up{{{J}}})', "short", [("red", None), ("orange", 2), ("green", 3)], "Brokers whose JMX exporter answers (3 expected)."),
    ("Active controller", f'sum(kafka_controller_kafkacontroller_activecontrollercount{{{J}}})', "short", ONE, "Must be exactly 1."),
    ("Offline partitions", f'max(kafka_controller_kafkacontroller_offlinepartitionscount{{{J}}})', "short", RED0, "Partitions without a leader."),
    ("Under-min-ISR", f'sum(kafka_server_replicamanager_underminisrpartitioncount{{{J}}})', "short", RED0, "acks=all writes to these fail."),
    ("Under-replicated", f'sum(kafka_server_replicamanager_underreplicatedpartitions{{{J}}})', "short", [("green", None), ("orange", 1)], "Normal briefly during restarts."),
    ("Fenced brokers", f'max(kafka_controller_kafkacontroller_fencedbrokercount{{{J}}})', "short", RED0, ""),
])
stats([
    ("Topics", f'max(kafka_controller_kafkacontroller_globaltopiccount{{{J}}})', "short", [("blue", None)], ""),
    ("Partitions", f'max(kafka_controller_kafkacontroller_globalpartitioncount{{{J}}})', "short", [("blue", None)], ""),
    ("Messages in", f'sum(rate(kafka_server_brokertopicmetrics_messagesin_total{{{J}, topic=""}}[5m]))', "ops", [("blue", None)], "Messages per second, all topics."),
    ("Bytes in", f'sum(rate(kafka_server_brokertopicmetrics_bytesin_total{{{J}, topic=""}}[5m]))', "Bps", [("blue", None)], ""),
    ("Bytes out", f'sum(rate(kafka_server_brokertopicmetrics_bytesout_total{{{J}, topic=""}}[5m]))', "Bps", [("blue", None)], ""),
    ("Produce success", PRODUCE_OK, "percentunit", [("red", None), ("orange", 0.99), ("green", 0.999)], "SLI: share of successful produce requests (5 min)."),
])
ts("Messages in per broker", [target(f'sum by (node) (rate(kafka_server_brokertopicmetrics_messagesin_total{{{JN}, topic=""}}[5m]))', "{{node}}")], "ops", 12, 0)
ts("Bytes in / out per broker", [
    target(f'sum by (node) (rate(kafka_server_brokertopicmetrics_bytesin_total{{{JN}, topic=""}}[5m]))', "in {{node}}"),
    target(f'sum by (node) (rate(kafka_server_brokertopicmetrics_bytesout_total{{{JN}, topic=""}}[5m]))', "out {{node}}"),
], "Bps", 12, 12)
y[0] += 8
ts("Firing alerts", [target('sum by (alertname, severity) (ALERTS{alertstate="firing", cluster="$cluster"})', "{{alertname}} ({{severity}})")],
   "short", 24, 0, "Alerts from kafka-alerts.yml in this cluster (needs the rules loaded in this Prometheus).")
y[0] += 8

# --- Brokers and disks ------------------------------------------------------
row("Brokers and disks")
ts("Request handler idle", [target(f'kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent{{{JN}}}', "{{node}}")],
   "percentunit", 12, 0, "Below 40% warning, below 20% critical.")
ts("Network processor idle", [target(f'avg by (node) (kafka_network_processor_idlepercent{{{JN}}})', "{{node}}")],
   "percentunit", 12, 12, "Average over the network threads.")
y[0] += 8
ts("Produce p99 (broker side)", [target(f'kafka_network_requestmetrics_total_time_ms{{{JN}, request="Produce", quantile="0.99"}}', "{{node}}")],
   "ms", 12, 0, "Includes the wait for acks=all replication.")
ts("Fetch p99 (consumers)", [target(f'kafka_network_requestmetrics_total_time_ms{{{JN}, request="FetchConsumer", quantile="0.99"}}', "{{node}}")],
   "ms", 12, 12, "Includes long-poll waiting (fetch.max.wait.ms, 500 ms by default).")
y[0] += 8
ts("Requests per second by type", [target(f'sum by (request) (rate(kafka_network_requestmetrics_requests_total{{{JN}}}[5m])) > 0', "{{request}}")],
   "reqps", 12, 0)
ts("ISR shrinks / expands", [
    target(f'sum by (node) (rate(kafka_server_replicamanager_isrshrinks_total{{{JN}}}[5m]))', "shrinks {{node}}"),
    target(f'sum by (node) (rate(kafka_server_replicamanager_isrexpands_total{{{JN}}}[5m]))', "expands {{node}}"),
], "short", 12, 12, "Frequent changes: unstable network or an overloaded broker.")
y[0] += 8
ts("Partitions and leaders per broker", [
    target(f'kafka_server_replicamanager_partitioncount{{{JN}}}', "partitions {{node}}"),
    target(f'kafka_server_replicamanager_leadercount{{{JN}}}', "leaders {{node}}"),
], "short", 12, 0, "Leaders should be spread evenly.")
ts("Replication lag (max, messages)", [target(f'kafka_server_replicafetchermanager_maxlag{{{JN}, clientid="Replica"}}', "{{node}}")],
   "short", 12, 12, "How far this broker's follower replicas are behind the leaders.")
y[0] += 8
ts("JVM heap used / max", [
    target(f'jvm_memory_heap_used_bytes{{{JN}}}', "used {{node}}"),
    target(f'jvm_memory_heap_max_bytes{{{JN}}}', "max {{node}}"),
], "bytes", 12, 0)
ts("GC time", [target(f'sum by (node) (rate(jvm_gc_collection_seconds_sum{{{JN}}}[5m]))', "{{node}}")],
   "percentunit", 12, 12, "Share of time spent in garbage collection.")
y[0] += 8
ts("/data used", [target(f'1 - node_filesystem_avail_bytes{{{NODE}, mountpoint="/data"}} / node_filesystem_size_bytes{{{NODE}, mountpoint="/data"}}', "{{node}}")],
   "percentunit", 8, 0, "Warning above 75%, critical above 85%.")
ts("Disk busy (/data device)", [target(f'max by (node) (rate(node_disk_io_time_seconds_total{{{NODE}}}[5m]))', "{{node}}")],
   "percentunit", 8, 8, "Busiest disk per host.")
ts("CPU used", [target(f'1 - avg by (node) (rate(node_cpu_seconds_total{{{NODE}, mode="idle"}}[5m]))', "{{node}}")],
   "percentunit", 8, 16)
y[0] += 8
ts("KRaft: metadata offset per node", [target(f'kafka_server_raft_metrics_log_end_offset{{{JN}}}', "{{node}}")],
   "short", 12, 0, "The lines must stay together; a node falling behind is not replicating metadata.")
ts("KRaft: commit latency", [target(f'kafka_server_raft_metrics_commit_latency_avg{{{JN}}}', "{{node}}")],
   "ms", 12, 12)
y[0] += 8

# --- Topics -----------------------------------------------------------------
row("Topics and partitions", collapsed=False)
ts("Messages in per topic", [target(f'sum by (topic) (rate(kafka_server_brokertopicmetrics_messagesin_total{{{J}, topic=~"$topic", topic!=""}}[5m]))', "{{topic}}")],
   "ops", 12, 0)
ts("Bytes in per topic", [target(f'sum by (topic) (rate(kafka_server_brokertopicmetrics_bytesin_total{{{J}, topic=~"$topic", topic!=""}}[5m]))', "{{topic}}")],
   "Bps", 12, 12)
y[0] += 8
table("Size on disk per topic (all replicas)", f'sort_desc(sum by (topic) (kafka_log_log_size_bytes{{{J}, topic=~"$topic"}}))',
      8, 0, "Sum over the three replicas; divide by 3 for the data size.", value_name="Size", unit="bytes")
table("Partitions per topic", f'sum by (topic) (kafka_topic_partitions{{{LAG}, topic=~"$topic"}})', 8, 8, value_name="Partitions")
table("Under-replicated partitions per topic", f'sum by (topic) (kafka_topic_partition_under_replicated_partition{{{LAG}, topic=~"$topic"}}) > 0',
      8, 16, "Empty when all is well.", value_name="Under-replicated")
y[0] += 8

# --- Consumer groups --------------------------------------------------------
row("Consumer groups and lag")
ts("Lag per group and topic", [target(LAG_BY, "{{consumergroup}} / {{topic}}")],
   "short", 12, 0, "Messages not yet consumed. Growing for 15 min: critical alert.")
ts("Consumption rate per group", [target(f'sum by (consumergroup) (rate(kafka_consumergroup_current_offset_sum{{{LAG}, consumergroup=~"$consumergroup"}}[5m]))', "{{consumergroup}}")],
   "ops", 12, 12, "Committed messages per second.")
y[0] += 8
table("Lag now", f'sort_desc({LAG_BY})', 12, 0, value_name="Lag")
table("Members per group", f'sum by (consumergroup) (kafka_consumergroup_members{{{LAG}, consumergroup=~"$consumergroup"}})', 12, 12,
      "0 members with lag: the consumer is stopped.", value_name="Members")
y[0] += 8

# --- Kafka Connect ----------------------------------------------------------
row("Kafka Connect")
stats([
    ("Worker up", f'sum(up{{{CON}}})', "short", [("red", None), ("green", 1)], ""),
    ("Connectors", f'count(kafka_connect_connector_status{{{CON}}} == 1)', "short", [("blue", None)], ""),
    ("Failed connectors", f'count(kafka_connect_connector_status{{{CON}, status="failed"}} == 1)', "short", RED0, ""),
    ("Failed tasks", f'sum(kafka_connect_worker_connector_connector_failed_task_count{{{CON}}})', "short", RED0, ""),
])
STATE = [{"type": "value", "options": {"1": {"text": "yes"}, "0": {"text": "no"}}}]
table("Connector status", f'kafka_connect_connector_status{{{CON}}} == 1', 12, 0, "One row per connector with its state.", value_name="State",
      mappings=STATE)
table("Tasks per connector", f'sum by (connector) (kafka_connect_worker_connector_connector_running_task_count{{{CON}}})', 6, 12, value_name="Running")
table("Failed tasks per connector", f'sum by (connector) (kafka_connect_worker_connector_connector_failed_task_count{{{CON}}}) > 0', 6, 18,
      "Empty when all is well.", value_name="Failed")
y[0] += 8
ts("Record errors", [target(f'sum by (connector) (increase(kafka_connect_task_error_total_record_errors{{{CON}}}[10m]))', "{{connector}}")],
   "short", 12, 0, "Records that failed conversion or transforms in the last 10 minutes.")
ts("Records read by sink tasks", [target(f'sum by (connector) (kafka_connect_sink_task_sink_record_read_rate{{{CON}}})', "{{connector}}")],
   "ops", 12, 12)
y[0] += 8

# --- Client errors ----------------------------------------------------------
row("Client errors")
ts("Failed produce requests per topic", [target(f'sum by (topic) (rate(kafka_server_brokertopicmetrics_failedproducerequests_total{{{J}, topic=~"$topic", topic!=""}}[5m])) > 0', "{{topic}}")],
   "reqps", 12, 0, "Topics map to the systems that write to them.")
ts("Failed fetch requests per topic", [target(f'sum by (topic) (rate(kafka_server_brokertopicmetrics_failedfetchrequests_total{{{J}, topic=~"$topic", topic!=""}}[5m])) > 0', "{{topic}}")],
   "reqps", 12, 12)
y[0] += 8
ts("Rejected bytes per topic (message too large)", [target(f'sum by (topic) (rate(kafka_server_brokertopicmetrics_bytesrejected_total{{{J}, topic=~"$topic", topic!=""}}[5m])) > 0', "{{topic}}")],
   "Bps", 12, 0)
ts("Produce success ratio", [target(PRODUCE_OK, "produce success")],
   "percentunit", 12, 12, "SLI. Authorization denials are in /var/log/kafka/kafka-authorizer.log, not in metrics.", min0=False)
y[0] += 8


def var(name, query, multi, include_all, label):
    return {"name": name, "label": label, "type": "query", "datasource": DS, "definition": query,
            "query": {"query": query, "refId": name}, "refresh": 2, "sort": 1,
            "multi": multi, "includeAll": include_all, "allValue": ".*" if include_all else None,
            "current": {}}


dashboard = {
    "title": "Kafka - Integration Resilience",
    "uid": "kafka-integration-resilience",
    "tags": ["kafka"],
    "timezone": "browser",
    "editable": True,
    "schemaVersion": 39,
    "time": {"from": "now-6h", "to": "now"},
    "refresh": "1m",
    "graphTooltip": 1,
    "annotations": {"list": []},
    "templating": {"list": [
        {"name": "datasource", "label": "Prometheus", "type": "datasource", "query": "prometheus", "current": {}},
        var("cluster", 'label_values(up{job=~"int-res-.+-kafka-jmx"}, cluster)', False, False, "Cluster"),
        var("node", 'label_values(up{job=~"int-res-.+-kafka-jmx", cluster="$cluster"}, node)', True, True, "Broker"),
        var("topic", 'label_values(kafka_topic_partitions{job=~"int-res-.+-kafka-lag", cluster="$cluster"}, topic)', True, True, "Topic"),
        var("consumergroup", 'label_values(kafka_consumergroup_members{job=~"int-res-.+-kafka-lag", cluster="$cluster"}, consumergroup)', True, True, "Consumer group"),
    ]},
    "panels": panels,
}


def drop_nulls(value):
    """Grafana 12's resource validation rejects null where it expects a type."""
    if isinstance(value, dict):
        return {k: drop_nulls(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [drop_nulls(v) for v in value]
    return value


dashboard = drop_nulls(dashboard)
json.dump(dashboard, open(sys.argv[1], "w"), indent=2)
# The same dashboard as a Grafana 12 resource (apiVersion/kind/metadata/spec),
# for editors and APIs that expect that form instead of the classic model
if len(sys.argv) > 2:
    spec = {k: v for k, v in dashboard.items() if k != "uid"}
    json.dump({"apiVersion": "dashboard.grafana.app/v1beta1", "kind": "Dashboard",
               "metadata": {"name": dashboard["uid"]}, "spec": spec}, open(sys.argv[2], "w"), indent=2)
print(len([p for p in panels if p["type"] != "row"]), "panels")
