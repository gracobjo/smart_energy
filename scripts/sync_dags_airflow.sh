#!/usr/bin/bash
# Sincroniza DAGs de orquestacion/ a AIRFLOW_HOME/dags.
# Uso: ./scripts/sync_dags_airflow.sh
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DAGS_SRC="$BASE/orquestacion"
DAGS_DST="${AIRFLOW_HOME:-$HOME/airflow}/dags"

if [[ ! -d "$DAGS_DST" ]]; then
  echo "Creando $DAGS_DST"
  mkdir -p "$DAGS_DST"
fi
cp -v "$DAGS_SRC"/dag_*.py "$DAGS_DST/"
echo "DAGs sincronizados. Define SMART_ENERGY_HOME=$BASE en el entorno de Airflow si los DAGs están en una copia."
