# Transactional Analytics Lakehouse Pipeline

Проект реализует lakehouse-пайплайн для обработки транзакций, отмен и курсов валют с последующей загрузкой агрегированного OLAP-куба в ClickHouse для визуализации в Superset.

---

# Архитектура

Поток данных:

```text
Public S3
    -> MinIO raw layer
    -> Iceberg silver layer
    -> Iceberg gold layer
    -> ClickHouse OLAP cube
    -> Superset dashboards
'''

Основные слои
Raw

Исходные JSONL-файлы, сохранённые в MinIO.

Silver

Очищенные Iceberg-таблицы:

local.silver.transactions_clean
local.silver.cancellations_clean
local.silver.exchange_rates_clean
Gold

Аналитические Iceberg-таблицы:

local.gold.dim_exchange_rate_intervals
local.gold.fct_transactions_enriched
Serving Layer

ClickHouse-куб для BI:

maria_riabkova.transactions_cube_hourly
Источники данных
Transactions

Файлы появляются каждые 10 минут:

https://storage.yandexcloud.net/npl-de18-lab8-data/day=YYYY-MM-DD/slot=HH-MM/transactions.jsonl
Exchange rates

Курсы валют загружаются несколько раз в день:

https://storage.yandexcloud.net/npl-de18-lab8-data/exchange_rates/day=YYYY-MM-DD/rates.jsonl
Cancellations

Отмены загружаются дневными файлами:

https://storage.yandexcloud.net/npl-de18-lab8-data/cancellations/day=YYYY-MM-DD/cancellations.jsonl
Оркестрация

Dagster assets находятся в:

transaction_lakehouse/assets
Основные assets
raw_transactions_file
silver_transactions_clean
raw_exchange_rates_file
silver_exchange_rates_clean
gold_exchange_rate_intervals
raw_cancellations_file
silver_cancellations_clean
gold_transactions_enriched
Scheduling
Transactions

Транзакции загружаются каждые 10 минут, но pipeline берёт предыдущий завершённый slot, чтобы не читать недозаписанный файл.

Exchange rates

Курсы валют проверяются 3 раза в день:

10:00 МСК
13:00 МСК
16:00 МСК
Gold Layer
local.gold.dim_exchange_rate_intervals

Таблица интервалов действия курсов валют.

Правило выбора курса
Для транзакций после первого известного курса используется последний валидный курс:
rate_timestamp <= transaction.created_at
Для транзакций раньше первого курса используется первый доступный курс после транзакции.

Такие строки помечаются:

backfill_next
Если курс не найден:
amount_tgrk = NULL

но строка не удаляется.

local.gold.fct_transactions_enriched

Широкая fact-таблица.

Одна строка соответствует одной строке из:

local.silver.transactions_clean

но уже обогащённой:

users / test users
promo_codes
exchange rate intervals
cancellations
DQ-флагами
business flags
amount conversion в TGRK

Решения по качеству данных
Дублирующиеся transaction_id

Проверка показала, что одинаковый transaction_id может встречаться:

у разных пользователей;
в разные даты;
с разными суммами;
с разными валютами;
с разными типами транзакций.

Поэтому transaction_id не считается надёжным уникальным бизнес-ключом.

Принятое решение

Строки не удаляются.

Вместо этого рассчитываются:

duplicate_count
duplicate_rank
is_duplicate_transaction
Интерпретация

is_duplicate_transaction трактуется как:

data quality warning

а не как признак удаления строки.

Для всех строк:

is_canonical_transaction = true

В ClickHouse отдельно считается:

duplicate_transaction_id_rows_count
Missing user

Если отсутствуют user_id или user_uuid, строка не удаляется.

Пользовательские сегменты
anonymous — user_id IS NULL
test — пользователь найден в test users
real — пользователь не тестовый

Negative / zero amount

Отрицательные и нулевые суммы не ломают pipeline.

Используются флаги:

negative_amount
zero_amount
Late-arriving cancellations

Отмены могут приходить позже даты транзакции.

Поэтому:

gold enriched пересчитывается rolling window;
historical backfill выполняется отдельным запуском.

ClickHouse Cube

Таблица:

maria_riabkova.transactions_cube_hourly
Гранулярность

Куб агрегируется по:

дате;
часу;
валюте;
типу транзакции;
статусу;
сегменту пользователя;
признаку отмены;
причине отмены;
стратегии выбора курса.
Гибкое определение покупки

Куб хранит не одно жёсткое определение покупки, а набор измерений:
transaction_type
status
is_purchase_type
is_completed_status
is_completed_purchase

Поэтому в Superset можно менять бизнес-определение покупки:

все транзакции;
только transaction_type = 'purchase';
только status = 'completed';
purchase AND completed.
Запуск
Настройка env

cp .env.example .env
nano .env

Запуск Superset
docker compose -f docker-compose.superset.yml up -d --build
Historical Backfill
Gold enriched
spark-submit \
  --jars "$SPARK_JARS" \
  --conf spark.sql.codegen.wholeStage=false \
  --conf spark.sql.codegen.factoryMode=NO_CODEGEN \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.sql.shuffle.partitions=8 \
  spark_jobs/build_transactions_enriched.py \
  --start-date 2026-04-27 \
  --end-date 2026-05-25

Export cube to ClickHouse
spark-submit \
  --jars "$SPARK_JARS" \
  --conf spark.sql.codegen.wholeStage=false \
  --conf spark.sql.codegen.factoryMode=NO_CODEGEN \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.sql.shuffle.partitions=8 \
  spark_jobs/export_transactions_cube_to_clickhouse.py \
  --start-date 2026-04-27 \
  --end-date 2026-05-25

Почему отключены Spark codegen/AQE

Для wide gold enrichment job на single-node VM связка:

Spark
Iceberg
OpenJDK 17

нестабильно падала с JVM:

SIGSEGV

Поэтому для gold/export jobs отключены:

spark.sql.codegen.wholeStage=false
spark.sql.codegen.factoryMode=NO_CODEGEN
spark.sql.adaptive.enabled=false
spark.sql.shuffle.partitions=8

Это не меняет бизнес-логику pipeline, а делает выполнение стабильнее на ограниченной VM.




