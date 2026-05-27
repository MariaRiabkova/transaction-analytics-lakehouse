import os
import subprocess
from pathlib import Path

import dagster as dg

from transaction_lakehouse.assets.exchange_rates import exchange_rates_partitions_def


PROJECT_DIR = Path("/home/ubuntu/lab8_lakehouse")
SPARK_SUBMIT = os.environ.get("SPARK_SUBMIT", "spark-submit")
SPARK_JARS = os.environ["SPARK_JARS"]


@dg.asset(
    group_name="exchange_rates",
    partitions_def=exchange_rates_partitions_def,
    deps=[
        dg.AssetKey("silver_exchange_rates_clean"),
    ],
)
def gold_exchange_rate_intervals(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    partition_day = context.partition_key

    script_path = PROJECT_DIR / "spark_jobs" / "build_exchange_rate_intervals.py"

    cmd = [
        SPARK_SUBMIT,
        "--jars",
        SPARK_JARS,
        str(script_path),
        "--start-date",
        partition_day,
        "--end-date",
        partition_day,
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
            f"spark-submit failed with exit code {result.returncode}"
        )

    return dg.MaterializeResult(
        metadata={
            "partition_day": partition_day,
            "source_table": "local.silver.exchange_rates_clean",
            "target_table": "local.gold.dim_exchange_rate_intervals",
            "spark_submit": SPARK_SUBMIT,
            "spark_jars": SPARK_JARS,
            "spark_job": str(script_path),
        }
    )
