import dagster as dg

from transaction_lakehouse.assets.cancellations import (
    cancellations_catchup_sensor,
    cancellations_daily_job,
    raw_cancellations_file,
    silver_cancellations_clean,
)
from transaction_lakehouse.assets.dictionaries import (
    dictionaries_iceberg_tables,
    raw_dictionaries_files,
)
from transaction_lakehouse.assets.exchange_rates import (
    exchange_rates_catchup_sensor,
    exchange_rates_daily_job,
    exchange_rates_intraday_schedule,
    raw_exchange_rates_file,
    silver_exchange_rates_clean,
)
from transaction_lakehouse.assets.gold_exchange_rates import (
    gold_exchange_rate_intervals,
)
from transaction_lakehouse.assets.gold_transactions_enriched import (
    gold_transactions_enriched,
)
from transaction_lakehouse.assets.transactions import (
    raw_transactions_file,
    silver_transactions_clean,
    transactions_ingest_job,
)
from transaction_lakehouse.resources.minio import make_minio_resource


defs = dg.Definitions(
    assets=[
        raw_dictionaries_files,
        dictionaries_iceberg_tables,

        raw_cancellations_file,
        silver_cancellations_clean,

        raw_exchange_rates_file,
        silver_exchange_rates_clean,
        gold_exchange_rate_intervals,

        raw_transactions_file,
        silver_transactions_clean,

        gold_transactions_enriched,
    ],
    resources={
        "minio": make_minio_resource(),
    },
    jobs=[
        cancellations_daily_job,
        exchange_rates_daily_job,
        transactions_ingest_job,
    ],
    sensors=[
        cancellations_catchup_sensor,
        exchange_rates_catchup_sensor,
        # transactions_catchup_sensor временно выключен
    ],
    schedules=[
        exchange_rates_intraday_schedule,
        # transactions_ingest_schedule временно выключен
    ],
)
