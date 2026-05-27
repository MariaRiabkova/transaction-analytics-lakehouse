import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import dagster as dg


PROJECT_DIR = Path("/home/ubuntu/lab8_lakehouse")
SPARK_SUBMIT = os.environ.get("SPARK_SUBMIT", "spark-submit")
SPARK_JARS = os.environ["SPARK_JARS"]

MOSCOW_TZ = ZoneInfo("Europe/Moscow")

SAFE_SPARK_CONF = [
    "--conf",
    "spark.sql.codegen.wholeStage=false",
    "--conf",
    "spark.sql.codegen.factoryMode=NO_CODEGEN",
    "--conf",
    "spark.sql.adaptive.enabled=false",
    "--conf",
    "spark.sql.shuffle.partitions=8",
]


def _default_gold_window() -> tuple[str, str]:
    today = datetime.now(MOSCOW_TZ).date()
    start_date = today - timedelta(days=2)
    end_date = today
    return start_date.isoformat(), end_date.isoformat()


@dg.asset(
    group_name="gold",
    deps=[
        dg.AssetKey("silver_transactions_clean"),
        dg.AssetKey("silver_cancellations_clean"),
        dg.AssetKey("dictionaries_iceberg_tables"),
        dg.AssetKey("gold_exchange_rate_intervals"),
    ],
)
def gold_transactions_enriched(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    start_date, end_date = _default_gold_window()

    script_path = PROJECT_DIR / "spark_jobs" / "build_transactions_enriched.py"

    cmd = [
        SPARK_SUBMIT,
        "--jars",
        SPARK_JARS,
        *SAFE_SPARK_CONF,
        str(script_path),
        "--start-date",
        start_date,
        "--end-date",
        end_date,
    ]

    context.log.info("Running command: " + " ".join(cmd))

    result = subprocess.run(
        cmd,
        cwd=str(PROJECT_DIR),
        text=True,
        capture_output=True,
        env=os.environ.copy(),
    )

    if result.stdout:
        context.log.info(result.stdout)

    if result.stderr:
        context.log.warning(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f"spark-submit failed for build_transactions_enriched.py "
            f"with exit code {result.returncode}"
        )

    return dg.MaterializeResult(
        metadata={
            "target_table": "local.gold.fct_transactions_enriched",
            "source_table": "local.silver.transactions_clean",
            "depends_on_rates_table": "local.gold.dim_exchange_rate_intervals",
            "window_start": start_date,
            "window_end": end_date,
            "spark_job": str(script_path),
            "spark_submit": SPARK_SUBMIT,
            "safe_spark_conf": SAFE_SPARK_CONF,
        }
    )
