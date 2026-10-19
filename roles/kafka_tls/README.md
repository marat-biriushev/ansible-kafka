# kafka_tls

TLS material for the cluster. Takes ready PEM files from the control node
and builds the PKCS12 stores on the brokers; on the services VM it only
places the CA chain for the containers.

Files expected in `kafka_tls_source_dir` (control node, `certs/<env>/`):

| File | Content |
|---|---|
| `ca.pem` | CA chain, PEM, every certificate of the chain |
| `<inventory_hostname>.crt` | node certificate, PEM |
| `<inventory_hostname>.key` | node private key, PEM, not encrypted |

On each broker: `/etc/kafka/tls/{ca.pem,node.crt,node.key,keystore.p12,truststore.p12}`,
owner `kafka`, keys and stores `0600`. The keystore is built with
`openssl pkcs12 -export`, the truststore with `keytool -importcert`
(one alias per CA certificate). A store is rebuilt when a PEM changed,
when it is missing, or when it no longer opens with the vault password
(password rotation); a rebuild marks the broker for a rolling restart.

`kafka_tls_selfsigned: true` (used on TEST and PROD, each with its own CA)
generates a CA and node certificates with plain `openssl` (3.0+) on
the control node into the same directory, with the same SANs and
`extendedKeyUsage = serverAuth, clientAuth`. No Ansible collection is
needed. A node certificate is reissued when it is missing or expires within
`kafka_tls_selfsigned_renew_days`; delete a `.crt` to reissue it earlier
(for example after changing the SANs).

The certificate expiry (read with `openssl` on the broker) is written to
`/var/lib/node_exporter/textfile/kafka_tls.prom` as
`kafka_tls_certificate_not_after_seconds` for the expiry alerts.

The role also creates the `kafka` system user if it does not exist yet,
because the stores must be owned by it before the `kafka` role runs.

## Variables

All of them are in `defaults/main.yml` and validated by `meta/argument_specs.yml`.

| Variable | Default | Description |
|---|---|---|
| `kafka_tls_source_dir` | `{{ playbook_dir }}/../certs` | Directory on the control node with ca.pem, <host>.crt, <host>.key. |
| `kafka_tls_selfsigned` | `false` | Generate a CA and node certificates into kafka_tls_source_dir. |
| `kafka_tls_selfsigned_ca_cn` | `Kafka TEST CA` | CN of the self-signed CA. |
| `kafka_tls_selfsigned_ca_days` | `3650` | Validity of the self-signed CA. |
| `kafka_tls_selfsigned_cert_days` | `730` | Validity of self-signed node certificates. |
| `kafka_tls_selfsigned_renew_days` | `30` | Reissue a self-signed node certificate when it expires within this many days. |
| `kafka_tls_openssl` | `openssl` | openssl binary on the control node (3.0+) for the self-signed certificates. |
| `kafka_tls_broker_group` | `kafka` | Group that gets keystore and truststore. |
| `kafka_tls_client_group` | `kafka_services` | Group that gets only the CA file. |
| `kafka_tls_domain` | `{{ kafka_domain \| default('') }}` | DNS domain for the certificate names. |
| `kafka_tls_fqdn` | `(see defaults/main.yml)` | Certificate CN and first SAN. |
| `kafka_tls_bootstrap_name` | `""` | Client bootstrap DNS name, added to the SANs. |
| `kafka_tls_san` | `(see defaults/main.yml)` | Subject alternative names (FQDN, short name, bootstrap name, IP). |
| `kafka_tls_owner` | `kafka` | Owner of the TLS files on the brokers. |
| `kafka_tls_dir` | `/etc/kafka/tls` | TLS directory on the brokers. |
| `kafka_tls_keystore` | `{{ kafka_tls_dir }}/keystore.p12` | PKCS12 keystore built by kafka_tls. |
| `kafka_tls_truststore` | `{{ kafka_tls_dir }}/truststore.p12` | PKCS12 truststore built by kafka_tls. |
| `kafka_tls_keystore_password` | `""` | Keystore and key password (vault). |
| `kafka_tls_truststore_password` | `""` | Truststore password (vault). |
| `kafka_tls_keytool` | `keytool` | keytool binary (comes with the JDK). |
| `kafka_tls_client_ca_file` | `/etc/kafka-services/ca.pem` | CA file on the services VM, mounted into the containers. |
| `kafka_tls_textfile_dir` | `/var/lib/node_exporter/textfile` | node_exporter textfile directory for the expiry metric; empty disables it. |
