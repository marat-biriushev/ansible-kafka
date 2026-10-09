# kafka_ui

Kafka UI (kafbat) as a Docker compose project in `/opt/kafka-ui/`, port
8080. Kafka over SASL_SSL / SCRAM-SHA-512 with the read-only `kafka-ui`
account, and the UI itself in read-only mode.

Web login is set with `kafka_ui_auth_type`:

* `LOGIN_FORM` (default on TEST): `kafka_ui_login_user` /
  `kafka_ui_login_password` (vault).
* `OAUTH2` or `LDAP`: put the Spring settings into `kafka_ui_auth_env`,
  for example
  `SPRING_SECURITY_OAUTH2_CLIENT_REGISTRATION_KEYCLOAK_CLIENTSECRET`;
  they go to the 0600 `secrets.env`, never into `compose.yml`.

## Variables

All of them are in `defaults/main.yml` and validated by `meta/argument_specs.yml`.

| Variable | Default | Description |
|---|---|---|
| `kafka_ui_image` | `(see defaults/main.yml)` | Image reference (Nexus mirror). |
| `kafka_ui_project_dir` | `/opt/kafka-ui` | Compose project directory. |
| `kafka_ui_port` | `8080` | Published HTTP port. |
| `kafka_ui_cluster_name` | `{{ kafka_env \| default('kafka') }}` | Cluster name shown in the UI. |
| `kafka_ui_bootstrap_servers` | `{{ kafka_bootstrap_servers }}` | Broker host:port list (CLIENT listener). |
| `kafka_ui_kafka_user` | `kafka-ui` | SCRAM account (read-only ACLs). |
| `kafka_ui_kafka_password` | `""` | Its password (vault). |
| `kafka_ui_ca_file` | `/etc/kafka-services/ca.pem` | CA file on the host, mounted read-only. |
| `kafka_ui_readonly` | `true` | Read-only mode in the UI. |
| `kafka_ui_schema_registry_url` | `""` | Schema Registry URL; empty hides it. |
| `kafka_ui_auth_type` | `LOGIN_FORM` | LOGIN_FORM, OAUTH2 or LDAP. |
| `kafka_ui_login_user` | `admin` | LOGIN_FORM user. |
| `kafka_ui_login_password` | `""` | LOGIN_FORM password (vault). |
| `kafka_ui_auth_env` | `{}` | Spring settings for OAUTH2/LDAP; written to the 0600 env file. |
| `kafka_ui_healthcheck` | `(see defaults/main.yml)` | Docker healthcheck test. |
| `kafka_ui_env_extra` | `{}` | Extra environment variables. |
