# kafka_users

Service SCRAM-SHA-512 accounts and their ACLs. Runs on one broker
(`hosts: kafka[0]`) with the admin tools over the INTERNAL listener and
`/etc/kafka/admin.properties`.

* **Accounts.** `kafka-configs --alter --add-config SCRAM-SHA-512=[...]`
  for an account that does not exist yet or whose password changed. A
  change is detected through a salted sha256 of the password kept in
  `/var/lib/kafka-ansible/scram/<name>.sha256` (root, 0600); the plain
  password is never stored. Re-runs with the same passwords change nothing.
* **ACLs.** `kafka-acls --list` is parsed (filter plugin
  `filter_plugins/kafka_acls.py`), compared with `kafka_users_acls`, and
  only missing bindings are added. Nothing is removed: deleting an entry
  from the list does not revoke it, do that by hand with
  `kafka-acls --remove`.

`admin` is created by `kafka-storage format` and is not managed here.
Application accounts and ACLs are out of scope (separate repository).

ACL entry format:

```yaml
kafka_users_acls:
  - principal: "User:kafka-ui"
    resource_type: topic        # topic | group | cluster | transactional-id
    name: "*"                   # not used for cluster
    pattern_type: literal       # literal | prefixed (default literal)
    operations: [Describe, Read]
    host: "*"                   # default "*"
    permission: allow           # allow | deny, default allow
```

## Variables

All of them are in `defaults/main.yml` and validated by `meta/argument_specs.yml`.

| Variable | Default | Description |
|---|---|---|
| `kafka_users_kafka_home` | `/opt/kafka` | Kafka home on the broker that runs the admin tools. |
| `kafka_users_admin_config` | `/etc/kafka/admin.properties` | admin.properties (mTLS) for the admin tools. |
| `kafka_users_bootstrap_server` | `(see defaults/main.yml)` | INTERNAL listener of this broker. |
| `kafka_users_scram` | `[]` | Service accounts, [{name, password}]. Passwords from the vault. |
| `kafka_users_scram_iterations` | `8192` | SCRAM iterations. |
| `kafka_users_state_dir` | `/var/lib/kafka-ansible/scram` | Where salted password hashes are kept to detect password changes. |
| `kafka_users_hash_salt` | `{{ kafka_cluster_id \| default('kafka') }}` | Salt of those hashes. |
| `kafka_users_acls` | `[]` | ACL bindings to ensure; missing ones are added, none are removed. |
