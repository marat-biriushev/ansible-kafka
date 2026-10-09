# node_exporter

Installs Prometheus node_exporter from `artifacts/node_exporter-<version>.linux-amd64.tar.gz`
(sha256 checked against `artifacts.yml`) into `/opt/node_exporter-<version>.linux-amd64/`
and runs it under systemd as `node_exporter` on port 9100, with the
textfile collector reading `/var/lib/node_exporter/textfile` (the
certificate expiry metric from `kafka_tls` lands there). Used on all hosts.

## Variables

All of them are in `defaults/main.yml` and validated by `meta/argument_specs.yml`.

| Variable | Default | Description |
|---|---|---|
| `node_exporter_version` | `1.12.1` | node_exporter version; must match artifacts.yml. |
| `node_exporter_file` | `node_exporter-{{ node_exporter_version }}.linux-amd64.tar.gz` | Tarball file name in artifacts/. |
| `node_exporter_artifacts_dir` | `(see defaults/main.yml)` | Directory with the offline artifacts on the control node. |
| `node_exporter_artifacts_manifest` | `(see defaults/main.yml)` | Path of artifacts.yml on the control node. |
| `node_exporter_install_root` | `/opt` | Where the tarball is unpacked. |
| `node_exporter_install_dir` | `(see defaults/main.yml)` | Versioned install directory. |
| `node_exporter_user` | `node_exporter` | Service account. |
| `node_exporter_listen_address` | `:9100` | --web.listen-address. |
| `node_exporter_textfile_dir` | `/var/lib/node_exporter/textfile` | --collector.textfile.directory. |
| `node_exporter_extra_args` | `[]` | Extra command-line flags. |
