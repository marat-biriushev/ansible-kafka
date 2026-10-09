# ТЗ: Ansible-роли для Integration Kafka

Репозиторий: https://github.com/marat-biriushev/ansible-kafka (сейчас пустой).
Задача: написать Ansible-код, который с нуля разворачивает кластер Apache Kafka (KRaft) и служебные сервисы на RHEL 10 для сред TEST и PROD, а также выполняет rolling restart без простоя.

Работай итерациями: сначала структура репозитория и инвентари, потом роли по одной. После каждой роли прогоняй `ansible-lint` и `ansible-playbook --syntax-check`. Если для решения нужен выбор, которого нет в ТЗ, бери вариант из раздела «Решения по умолчанию» и отмечай его в README.

---

## 1. Инфраструктура

Домен: `ipa.ipotekabank.uz`. ОС: RHEL 10. Все VM в кластере виртуализации A.

### TEST (VLAN 427)

| Host | IP | Роль | CPU / RAM | Диски |
|---|---|---|---|---|
| int-res-test-kafka01 | 172.31.56.211 | broker + controller | 4 / 16 ГБ | OS + data 200 ГБ |
| int-res-test-kafka02 | 172.31.56.212 | broker + controller | 4 / 16 ГБ | OS + data 200 ГБ |
| int-res-test-kafka03 | 172.31.56.213 | broker + controller | 4 / 16 ГБ | OS + data 200 ГБ |
| int-res-test-kafkasvc01 | 172.31.56.214 | служебные сервисы | 4 / 8 ГБ | OS |

### PROD (VLAN 431)

| Host | IP | Роль | CPU / RAM | Диски |
|---|---|---|---|---|
| int-res-prod-kafka01 | 172.31.60.111 | broker + controller | 8 / 32 ГБ | OS + data 500 ГБ |
| int-res-prod-kafka02 | 172.31.60.112 | broker + controller | 8 / 32 ГБ | OS + data 500 ГБ |
| int-res-prod-kafka03 | 172.31.60.113 | broker + controller | 8 / 32 ГБ | OS + data 500 ГБ |
| int-res-prod-kafkasvc01 | 172.31.60.114 | служебные сервисы | 4 / 8 ГБ | OS |

Bootstrap-имя для клиентов (DNS с несколькими A-записями на все брокеры среды): `int-res-<env>-kafka.ipa.ipotekabank.uz`. Создание DNS-записей вне скоупа, но имя должно быть в SAN сертификатов.

---

## 2. Целевая конфигурация Kafka

| Параметр | Значение |
|---|---|
| Версия Kafka | Последний стабильный релиз 4.x (проверь на downloads.apache.org), закрепить в переменной `kafka_version`. Scala 2.13. |
| Java | OpenJDK 21 headless из репозиториев RHEL (`java-21-openjdk-headless`) |
| Режим | KRaft combined: `process.roles=broker,controller` на 3 узлах |
| node.id | 1, 2, 3 (из host_vars) |
| broker.rack | Переменная в host_vars, по умолчанию имя узла (`kafka01`…); в README пояснить, что значение должно соответствовать физическому хосту гипервизора |
| Кворум | Динамический (KIP-853): `controller.quorum.bootstrap.servers`, форматирование через `--initial-controllers` |
| Replication | `default.replication.factor=3`, `min.insync.replicas=2`, `offsets.topic.replication.factor=3`, `transaction.state.log.replication.factor=3`, `transaction.state.log.min.isr=2` |
| Надёжность | `unclean.leader.election.enable=false`, `auto.create.topics.enable=false`, `controlled.shutdown.enable=true` |
| Чтение с фолловера | `replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector` |
| Хранилище | `log.dirs` на отдельном data-диске, `/data/kafka` |
| Heap | TEST 4 ГБ, PROD 6 ГБ (переменная `kafka_heap`) |

### Listeners

| Имя | Порт | Протокол | Назначение |
|---|---|---|---|
| CLIENT | 9092 | SASL_SSL, SCRAM-SHA-512 | Клиенты и служебные сервисы |
| INTERNAL | 9091 | SSL (mTLS, `ssl.client.auth=required`) | Межброкерная репликация, админ-операции из Ansible |
| CONTROLLER | 9093 | SSL (mTLS) | KRaft-кворум |

`advertised.listeners` — FQDN узла.

### Безопасность

| Что | Требование |
|---|---|
| TLS | TLS 1.2+, сертификаты внутреннего CA банка. Роль принимает готовые PEM (cert, key, CA chain) и сама собирает PKCS12 keystore/truststore. SAN: FQDN узла, короткое имя, IP, bootstrap-имя среды. |
| Self-signed режим | Только для TEST: флаг `kafka_tls_selfsigned: true` генерирует свой CA и сертификаты (`community.crypto`), чтобы стенд поднимался без заявки в CA. |
| Авторизация | `authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer`, `allow.everyone.if.no.acl.found=false`, `super.users` = CN сертификата брокеров + `User:admin` |
| SCRAM-пользователи | `admin` создаётся при форматировании (`kafka-storage format --add-scram`). Служебные: `schema-registry`, `kafka-ui`, `kafka-exporter`, `canary` — через `kafka-configs` после старта, идемпотентно. |
| ACL служебных учёток | Минимальные права (см. раздел 4), задаются переменной-списком и применяются идемпотентно (`kafka-acls --list` → сравнение → `--add`) |
| Секреты | Все пароли — в `inventories/<env>/group_vars/all/vault.yml` (ansible-vault). В репозиторий положить только `vault.yml.example`. Структуру переменных сделать такой, чтобы позже можно было заменить на lookup `community.hashi_vault`. |
| Логи авторизатора | `kafka.authorizer.logger=INFO` в отдельный файл (`kafka-authorizer.log`) для последующей отправки в SIEM |
| Права на файлы | Kafka работает от системного пользователя `kafka`; ключи и keystore — `0600`, владелец `kafka` |

---

## 3. Подготовка ОС (роль `common`)

| Что | Требование |
|---|---|
| Data-диск | Переменная `kafka_data_device` (например `/dev/sdb`). Создать XFS, если ФС нет (никогда не переформатировать существующую), смонтировать в `/data` с `noatime`, запись в fstab по UUID. |
| sysctl | `vm.swappiness=1`, `vm.dirty_ratio=80`, `vm.dirty_background_ratio=5`, `vm.max_map_count=262144`, `net.core.somaxconn=4096`, `net.ipv4.tcp_max_syn_backlog=4096` |
| limits | `nofile` 100000 для пользователя `kafka` (через `LimitNOFILE` в systemd unit) |
| Время | chrony установлен и включён; NTP-серверы — переменная |
| firewalld | Включён. Открыть: 9092 для `kafka_client_networks`; 9091, 9093 только между узлами кластера; 7071 и 9100 для `monitoring_networks`; на svc-VM — 8081, 8080, 9308 по соответствующим переменным |
| SELinux | Остаётся `enforcing`. Если нужны контексты или порты для podman — настроить через `community.general.sefcontext` / `seport` |
| /etc/hosts | Не трогать (DNS есть); опционально флаг `manage_etc_hosts: false` |

---

## 4. Служебные сервисы (роль `kafka_services`, хост kafkasvc01)

Запуск через **Podman + Quadlet** (`/etc/containers/systemd/*.container`), rootful, каждый сервис — systemd unit. Docker в RHEL 10 нет, docker-compose не использовать. Образы с закреплёнными тегами в переменных. Поддержать переменную `container_registry_mirror` для внутреннего зеркала.

| Сервис | Образ (по умолчанию) | Порт | Учётка | Минимальные ACL |
|---|---|---|---|---|
| Schema Registry | `confluentinc/cp-schema-registry` (переменная, т.к. вопрос лицензии открыт) | 8081 | `schema-registry` | All на топик `_schemas`; Read/Describe на группу `schema-registry`; DescribeConfigs на cluster |
| Kafka UI | `ghcr.io/kafbat/kafka-ui` | 8080 | `kafka-ui` | Describe/Read на все топики и группы (только чтение) |
| kafka-exporter | `danielqsj/kafka-exporter` | 9308 | `kafka-exporter` | Describe на топики, группы и cluster |
| Canary | `quay.io/strimzi/canary` | 8080 внутр. → 8082 | `canary` | All на топик `__strimzi_canary` и группу canary. Включается флагом `canary_enabled` |

Требования:
- Топик `_schemas` создаётся ролью заранее: 1 партиция, RF=3, `cleanup.policy=compact`, `min.insync.replicas=2`.
- Kafka UI: аутентификация через переменные (`kafka_ui_auth_type: LOGIN_FORM | OAUTH2 | LDAP`); на TEST по умолчанию `LOGIN_FORM`.
- Сертификат CA монтируется в контейнеры read-only; пароли — через Podman secrets или env-файл с правами `0600`, не в unit-файле.
- Healthcheck для каждого контейнера; `Restart=always`.

---

## 5. Мониторинг (роли `kafka` и `node_exporter`)

| Что | Требование |
|---|---|
| JMX Exporter | `jmx_prometheus_javaagent` (версия в переменной) как javaagent на порту 7071; конфиг с правилами для `kafka.server`, `kafka.controller`, `kafka.network`, `kafka.log`, `kafka.server:type=raft-metrics` |
| node_exporter | Бинарь + systemd на всех узлах, порт 9100 |
| Scrape-конфиг | Положить в `monitoring/vmagent-scrape.yml` готовый фрагмент `scrape_configs` для vmagent (job'ы kafka-jmx, node, kafka-exporter, schema-registry). Сам vmagent не разворачивать — он в существующем стеке. |
| Алерты | `monitoring/kafka-alerts.yml` в формате vmalert/Prometheus rules: OfflinePartitions > 0, ActiveControllerCount ≠ 1, UnderMinIsr > 0, UnderReplicated > 0 более 5 мин, FencedBrokerCount > 0, узел недоступен, диск /data > 75% / > 85%, consumer lag растёт, RequestHandlerAvgIdlePercent < 20%, сертификат истекает < 30 / < 7 дней, Schema Registry недоступен, canary error rate > 0. Имена метрик сверить с правилами JMX Exporter из этого же репозитория. |

---

## 6. Структура репозитория

```
ansible-kafka/
├── README.md
├── ansible.cfg
├── requirements.yml              # community.general, community.crypto, ansible.posix, containers.podman
├── inventories/
│   ├── test/
│   │   ├── hosts.yml             # группы: kafka, kafka_services
│   │   ├── host_vars/<host>.yml  # kafka_node_id, kafka_rack, kafka_data_device
│   │   └── group_vars/all/{main.yml,vault.yml.example}
│   └── prod/ (аналогично)
├── playbooks/
│   ├── site.yml                  # всё с нуля
│   ├── kafka.yml                 # только кластер
│   ├── services.yml              # только svc-VM
│   ├── rolling-restart.yml
│   └── users-acl.yml             # служебные SCRAM-учётки и ACL
├── roles/
│   ├── common/
│   ├── kafka/
│   ├── node_exporter/
│   └── kafka_services/
└── monitoring/
    ├── vmagent-scrape.yml
    └── kafka-alerts.yml
```

Каждая роль: `defaults/main.yml` с документированными переменными, `meta/argument_specs.yml`, `handlers/`, `templates/`, `README.md`.

---

## 7. Ключевая логика роли `kafka`

### Установка
1. Пользователь и группа `kafka`, каталоги `/opt/kafka` (симлинк на версию), `/etc/kafka`, `/data/kafka`, `/var/log/kafka`.
2. Дистрибутив: скачать с `kafka_download_url` (по умолчанию `https://archive.apache.org/dist/kafka/...`) с проверкой SHA512, **или** взять локальный файл, если задан `kafka_local_archive` (банк может быть без интернета).
3. Шаблоны `server.properties`, `log4j2.yaml` (в Kafka 4.x — log4j2), `jvm`-опций; systemd unit `kafka.service` с `LimitNOFILE`, `KAFKA_HEAP_OPTS`, `KAFKA_OPTS` (javaagent), `TimeoutStopSec=300`.

### Форматирование хранилища — главная опасность
- `cluster_id` и `directory_id` каждого узла задаются в inventory (vault или group_vars). Если не заданы, плейбук **останавливается** и печатает команду для генерации (`kafka-storage.sh random-uuid`). Не генерировать их «на лету» при каждом запуске.
- Форматировать **только если** нет `/data/kafka/meta.properties`. Если файл есть и `cluster.id` в нём не совпадает с inventory — `fail` с понятной ошибкой. Никакой опции «переформатировать» в роли нет.
- Команда (одинаковая строка `--initial-controllers` на всех трёх узлах):
  ```
  kafka-storage.sh format -t <cluster_id> -c /etc/kafka/server.properties \
    --initial-controllers "1@kafka01:9093:<dir1>,2@kafka02:9093:<dir2>,3@kafka03:9093:<dir3>" \
    --add-scram 'SCRAM-SHA-512=[name=admin,password=<vault>]'
  ```

### Первый запуск
Стартовать все 3 узла параллельно (иначе кворум не соберётся), затем ждать готовности: `kafka-metadata-quorum.sh describe --status` показывает 3 voter'а, `kafka-broker-api-versions.sh` отвечает на каждом узле. Для админ-команд Ansible генерирует `/etc/kafka/admin.properties` (INTERNAL listener, mTLS), права `0600`.

### Изменение конфигурации
Изменение шаблонов не перезапускает узлы сразу через handler. Handler только ставит факт `kafka_restart_required`, а перезапуск выполняется в конце плейбука по алгоритму rolling restart (`serial: 1`).

---

## 8. Rolling restart (`playbooks/rolling-restart.yml`)

`serial: 1`, для каждого узла:
1. Pre-check на любом узле: `kafka-topics --describe --under-replicated-partitions` пусто, `--unavailable-partitions` пусто, кворум 3/3 voter'а с допустимым лагом. Иначе `fail`.
2. Определить активного контроллера (`kafka-metadata-quorum describe --status`, поле LeaderId) и перезапускать его **последним** (переупорядочить хосты).
3. `systemctl restart kafka`, ожидание порта 9092/9091.
4. Ждать (retries с таймаутом, переменная, по умолчанию 15 мин), пока under-replicated снова пусто и узел вернулся в кворум.
5. После последнего узла — `kafka-leader-election --election-type preferred --all-topic-partitions`.

Флаг `--check` должен показывать порядок перезапуска без действий.

---

## 9. Решения по умолчанию

| Вопрос | По умолчанию |
|---|---|
| Аутентификация клиентов | SCRAM-SHA-512 |
| Schema Registry | Confluent-образ, переменная `schema_registry_image` (могут заменить на Apicurio/Karapace) |
| Контейнеры | Podman + Quadlet, rootful |
| Секреты | ansible-vault |
| Топики и ACL прикладных систем | **Вне скоупа** (будут в отдельном репо через CI/CD). Здесь только служебные учётки. |
| MirrorMaker 2, DR, Kafka Connect, Cruise Control | **Вне скоупа** (этап 2) |

---

## 10. Критерии приёмки

1. `ansible-lint` (профиль `production`) и `yamllint` без ошибок; `ansible-playbook --syntax-check` для всех плейбуков.
2. Повторный прогон `site.yml` на уже развёрнутом стенде: `changed=0`.
3. Molecule-сценарий для ролей `common` и `kafka` на образе AlmaLinux/Rocky 10 (3 контейнера, self-signed TLS): кластер поднимается, кворум 3/3, тестовый топик RF=3 создаётся, продюсер с `acks=all` от SCRAM-пользователя пишет, консьюмер читает.
4. Ручной тест на TEST, описанный в README: остановка одного узла → запись с `acks=all` продолжается; rolling restart проходит без ошибок у работающего продюсера (`kafka-producer-perf-test`).
5. README: порядок развёртывания с нуля, генерация `cluster_id`/`directory_id`, подготовка сертификатов (что запросить у CA, какие SAN), команды запуска каждого плейбука, описание всех переменных, известные ограничения.

---

## 11. Ограничения

- Не использовать `shell`/`command` там, где есть модуль. Если без них не обойтись (kafka-*.sh), задавать `changed_when`/`failed_when`.
- Не логировать секреты: `no_log: true` на задачах с паролями.
- Не коммитить реальные `vault.yml`, ключи и сертификаты; добавить `.gitignore`.
- Коммиты небольшие и логичные (структура → common → kafka → services → monitoring → docs).
