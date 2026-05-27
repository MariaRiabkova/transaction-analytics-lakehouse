#!/usr/bin/env bash
set -euo pipefail

cd /home/ubuntu/lab8_lakehouse

export DAGSTER_HOME=/home/ubuntu/lab8_lakehouse/dagster_home

exec /home/ubuntu/lab8_lakehouse/.venv_dagster/bin/dagster dev \
  -m transaction_lakehouse.definitions \
  --host 127.0.0.1 \
  --port 3000
