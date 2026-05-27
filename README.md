# Transaction Lakehouse Pipeline

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
