# Transactional Analytics Lakehouse Pipeline

Проект реализует **lakehouse-пайплайн для транзакционной аналитики**: данные о транзакциях, отменах и курсах валют загружаются в MinIO, обрабатываются в Iceberg-слоях, агрегируются в OLAP-куб ClickHouse и используются для визуализации в Superset.

---

## Содержание

- [Архитектура](#архитектура)
- [Слои данных](#слои-данных)
- [Источники данных](#источники-данных)
- [Оркестрация](#оркестрация)
- [Gold layer](#gold-layer)
- [Data Quality решения](#data-quality-решения)
- [ClickHouse cube](#clickhouse-cube)
- [Запуск проекта](#запуск-проекта)
- [Historical backfill](#historical-backfill)
- [Особенности Spark jobs на VM](#особенности-spark-jobs-на-vm)

---

## Архитектура

```text
Public S3
   |
   v
MinIO raw layer
   |
   v
Iceberg silver layer
   |
   v
Iceberg gold layer
   |
   v
ClickHouse OLAP cube
   |
   v
Superset dashboards
```

Пайплайн построен вокруг lakehouse-подхода: исходные файлы сохраняются в raw layer, затем очищаются и нормализуются в silver layer, после чего собираются аналитические gold-таблицы и витрина для BI.

---

## Слои данных

### Raw layer

Исходные JSONL-файлы сохраняются в MinIO без бизнес-трансформаций.

Raw layer используется как стабильная точка входа для повторной обработки данных и historical backfill.

### Silver layer

Очищенные Iceberg-таблицы:

```text
local.silver.transactions_clean
local.silver.cancellations_clean
local.silver.exchange_rates_clean
```

На этом слое выполняются базовые преобразования:

- приведение типов;
- нормализация timestamp/date полей;
- подготовка данных к join-операциям;
- сохранение технических и DQ-признаков.

### Gold layer

Аналитические Iceberg-таблицы:

```text
local.gold.dim_exchange_rate_intervals
local.gold.fct_transactions_enriched
```

Gold layer содержит бизнес-логику: интервалы курсов валют, enriched fact-таблицу транзакций, флаги отмен, пользовательские сегменты и признаки качества данных.

### Serving layer

Финальная BI-витрина хранится в ClickHouse:

```text
maria_riabkova.transactions_cube_hourly
```

Эта таблица используется как источник данных для Superset dashboards.

---

## Источники данных

### Transactions

Транзакции появляются каждые 10 минут:

```text
https://storage.yandexcloud.net/npl-de18-lab8-data/day=YYYY-MM-DD/slot=HH-MM/transactions.jsonl
```

### Exchange rates

Курсы валют загружаются несколько раз в день:

```text
https://storage.yandexcloud.net/npl-de18-lab8-data/exchange_rates/day=YYYY-MM-DD/rates.jsonl
```

### Cancellations

Отмены загружаются дневными файлами:

```text
https://storage.yandexcloud.net/npl-de18-lab8-data/cancellations/day=YYYY-MM-DD/cancellations.jsonl
```

---

## Оркестрация

Оркестрация реализована через Dagster assets.

Основной код assets находится в директории:

```text
transaction_lakehouse/assets
```

Основные assets:

```text
raw_transactions_file
silver_transactions_clean
raw_exchange_rates_file
silver_exchange_rates_clean
gold_exchange_rate_intervals
raw_cancellations_file
silver_cancellations_clean
gold_transactions_enriched
```

### Scheduling

#### Transactions

Транзакции загружаются каждые 10 минут.

Pipeline берёт **предыдущий завершённый slot**, чтобы не читать файл, который ещё может быть недозаписан.

#### Exchange rates

Курсы валют проверяются 3 раза в день:

```text
10:00 MSK
13:00 MSK
16:00 MSK
```

---

## Gold layer

### `local.gold.dim_exchange_rate_intervals`

Таблица интервалов действия курсов валют.

Для каждой валюты строятся интервалы, в рамках которых действует конкретный курс.

#### Правило выбора курса

Для транзакций после первого известного курса используется последний валидный курс:

```text
rate_timestamp <= transaction.created_at
```

Для транзакций раньше первого курса используется первый доступный курс после транзакции.

Такие строки помечаются стратегией:

```text
backfill_next
```

Если курс не найден:

```text
amount_tgrk = NULL
```

При этом строка транзакции не удаляется из fact-таблицы.

### `local.gold.fct_transactions_enriched`

Широкая fact-таблица транзакций.

Одна строка соответствует одной строке из:

```text
local.silver.transactions_clean
```

Таблица обогащается следующими данными:

- пользователи;
- test users;
- promo codes;
- интервалы курсов валют;
- отмены;
- DQ-флаги;
- бизнес-флаги;
- конвертация суммы в TGRK.

---

## Data Quality решения

### Дублирующиеся `transaction_id`

Проверка показала, что одинаковый `transaction_id` может встречаться:

- у разных пользователей;
- в разные даты;
- с разными суммами;
- с разными валютами;
- с разными типами транзакций.

Поэтому `transaction_id` не считается надёжным уникальным бизнес-ключом.

Принятое решение: строки не удаляются, а размечаются DQ-признаками.

Рассчитываются поля:

```text
duplicate_count
duplicate_rank
is_duplicate_transaction
```

`is_duplicate_transaction` трактуется как **data quality warning**, а не как причина для удаления строки.

Для всех строк:

```text
is_canonical_transaction = true
```

В ClickHouse отдельно считается метрика:

```text
duplicate_transaction_id_rows_count
```

### Missing user

Если отсутствует `user_id` или `user_uuid`, строка не удаляется.

Пользовательские сегменты:

| Segment | Условие |
|---|---|
| `anonymous` | `user_id IS NULL` |
| `test` | пользователь найден в test users |
| `real` | пользователь не является тестовым |

### Negative / zero amount

Отрицательные и нулевые суммы не ломают pipeline.

Для анализа используются флаги:

```text
negative_amount
zero_amount
```

### Late-arriving cancellations

Отмены могут приходить позже даты транзакции.

Поэтому enriched gold-таблица пересчитывается через rolling window, а historical backfill выполняется отдельным запуском.

---

## ClickHouse cube

Финальная таблица:

```text
maria_riabkova.transactions_cube_hourly
```

### Гранулярность

Куб агрегируется по следующим измерениям:

- дата;
- час;
- валюта;
- тип транзакции;
- статус;
- пользовательский сегмент;
- признак отмены;
- причина отмены;
- стратегия выбора курса.

### Гибкое определение покупки

Куб не фиксирует одно жёсткое бизнес-определение покупки.

Вместо этого сохраняется набор измерений:

```text
transaction_type
status
is_purchase_type
is_completed_status
is_completed_purchase
```

Поэтому в Superset можно анализировать разные определения покупки:

- все транзакции;
- только `transaction_type = 'purchase'`;
- только `status = 'completed'`;
- `purchase AND completed`.

---

## Запуск проекта

### Настройка `.env`

```bash
cp .env.example .env
nano .env
```

### Запуск Superset

```bash
docker compose -f docker-compose.superset.yml up -d --build
```

---

## Historical backfill

### Gold enriched

```bash
spark-submit \
  --jars "$SPARK_JARS" \
  --conf spark.sql.codegen.wholeStage=false \
  --conf spark.sql.codegen.factoryMode=NO_CODEGEN \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.sql.shuffle.partitions=8 \
  spark_jobs/build_transactions_enriched.py \
  --start-date 2026-04-27 \
  --end-date 2026-05-25
```

### Export cube to ClickHouse

```bash
spark-submit \
  --jars "$SPARK_JARS" \
  --conf spark.sql.codegen.wholeStage=false \
  --conf spark.sql.codegen.factoryMode=NO_CODEGEN \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.sql.shuffle.partitions=8 \
  spark_jobs/export_transactions_cube_to_clickhouse.py \
  --start-date 2026-04-27 \
  --end-date 2026-05-25
```

---

## Особенности Spark jobs на VM

Для wide gold enrichment job на single-node VM связка:

```text
Spark + Iceberg + OpenJDK 17
```

нестабильно падала с JVM error:

```text
SIGSEGV
```

Поэтому для gold/export jobs отключены:

```text
spark.sql.codegen.wholeStage=false
spark.sql.codegen.factoryMode=NO_CODEGEN
spark.sql.adaptive.enabled=false
spark.sql.shuffle.partitions=8
```

Это не меняет бизнес-логику pipeline, но делает выполнение стабильнее на ограниченной VM.





