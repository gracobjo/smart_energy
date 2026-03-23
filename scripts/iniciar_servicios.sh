#!/usr/bin/bash
# Arranca servicios base Smart Grid desde la raíz del repo (equivalente a Fase 0 en el dashboard):
#   HDFS, Kafka (KRaft), Cassandra, Airflow, API Swagger.
#
# Uso:
#   cd ~/smart_energy
#   ./scripts/iniciar_servicios.sh
#   ./scripts/iniciar_servicios.sh --only cassandra
#   ./scripts/iniciar_servicios.sh --only api
#
# Variables: HADOOP_HOME, KAFKA_HOME, AIRFLOW_HOME, API_SMART_GRID_PORT (8000)
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$BASE"

HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
AIRFLOW_HOME="${AIRFLOW_HOME:-$HOME/airflow}"

ONLY=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --only)
      shift
      ONLY="${1:-}"
      shift || true
      ;;
    -h|--help)
      echo "Uso: $0 [--only hdfs|kafka|cassandra|airflow|api]"
      exit 0
      ;;
    *)
      echo "Opción desconocida: $1" >&2
      exit 1
      ;;
  esac
done

_port_open() {
  local p="$1"
  (command -v nc &>/dev/null && nc -z -w1 127.0.0.1 "$p") 2>/dev/null && return 0
  (echo >/dev/tcp/127.0.0.1/"$p") &>/dev/null && return 0
  return 1
}

_should_run() {
  [[ -z "$ONLY" ]] || [[ "$ONLY" == "$1" ]]
}

_start_hdfs() {
  if _port_open 9000; then
    echo "=== HDFS: ya responde (RPC ~9000) ==="
    return 0
  fi
  echo "=== HDFS: arrancando ==="
  if hdfs dfs -ls / &>/dev/null; then
    echo "HDFS ya accesible vía CLI."
    return 0
  fi
  local start_all="$HADOOP_HOME/sbin/start-all.sh"
  local start_dfs="$HADOOP_HOME/sbin/start-dfs.sh"
  if [[ -x "$start_all" ]]; then
    "$start_all" || true
  elif [[ -x "$start_dfs" ]]; then
    "$start_dfs" || true
  else
    echo "AVISO: No encuentro $start_all ni $start_dfs. Define HADOOP_HOME." >&2
    return 1
  fi
  sleep 2
  if hdfs dfs -ls / &>/dev/null; then
    echo "HDFS OK."
  else
    echo "AVISO: HDFS puede tardar; comprueba: hdfs dfs -ls /" >&2
  fi
}

_start_kafka() {
  if _port_open 9092; then
    echo "=== Kafka: ya responde en 9092 ==="
    return 0
  fi
  local start_script="$KAFKA_HOME/bin/kafka-server-start.sh"
  local props="$KAFKA_HOME/config/kraft/server.properties"
  [[ -f "$props" ]] || props="$KAFKA_HOME/config/server.properties"
  if [[ ! -x "$start_script" ]] || [[ ! -f "$props" ]]; then
    echo "ERROR: Kafka no instalado en KAFKA_HOME=$KAFKA_HOME. Ejecuta: ./scripts/instalar_kafka_local.sh" >&2
    return 1
  fi
  echo "=== Kafka: arrancando (log /tmp/smart_grid_kafka_iniciar.log) ==="
  nohup "$start_script" "$props" >>/tmp/smart_grid_kafka_iniciar.log 2>&1 &
  echo "Kafka pid=$!"
}

_start_cassandra() {
  if _port_open 9042; then
    echo "=== Cassandra: ya responde en 9042 ==="
    return 0
  fi
  local cass="$BASE/cassandra/bin/cassandra"
  if [[ ! -x "$cass" ]]; then
    echo "ERROR: No ejecutable: $cass (instala Cassandra 5.x bajo $BASE/cassandra/)" >&2
    return 1
  fi
  local logf="/tmp/smart_grid_cassandra.log"
  echo "=== Cassandra: arrancando (cd $BASE && ./cassandra/bin/cassandra) ==="
  echo "    Log: $logf"
  nohup "$cass" >>"$logf" 2>&1 &
  echo "Cassandra pid=$! (espera 30–60 s y comprueba: nc -z 127.0.0.1 9042)"
}

_start_airflow() {
  export AIRFLOW_HOME
  local airflow_bin=""
  for venv in "$BASE/venv/bin/airflow" "$BASE/venv_transporte/bin/airflow" "$(command -v airflow 2>/dev/null)"; do
    [[ -x "${venv:-}" ]] && airflow_bin="$venv" && break
  done
  if [[ -z "$airflow_bin" ]]; then
    echo "AVISO: No encuentro airflow (venv, venv_transporte). Instala: pip install apache-airflow" >&2
    return 1
  fi
  local log_api="/tmp/smart_grid_airflow_api.log"
  local log_dag="/tmp/smart_grid_airflow_dag_processor.log"
  local log_sched="/tmp/smart_grid_airflow_scheduler.log"
  echo "=== Airflow: api-server + dag-processor + scheduler ==="
  if ! _port_open 8080; then
    nohup "$airflow_bin" api-server -H 0.0.0.0 -p 8080 >>"$log_api" 2>&1 &
    echo "  api-server pid=$!"
    sleep 2
  else
    echo "  api-server: ya responde en 8080"
  fi
  if ! pgrep -f "airflow dag-processor" >/dev/null 2>&1; then
    nohup "$airflow_bin" dag-processor >>"$log_dag" 2>&1 &
    echo "  dag-processor pid=$! (Airflow 3.x: parsea DAGs)"
  else
    echo "  dag-processor: ya en ejecución"
  fi
  sleep 2
  if ! pgrep -f "airflow scheduler" >/dev/null 2>&1; then
    nohup "$airflow_bin" scheduler >>"$log_sched" 2>&1 &
    echo "  scheduler pid=$!"
  else
    echo "  scheduler: ya en ejecución"
  fi
  echo "    Logs: $log_api, $log_dag, $log_sched"
  echo "    UI: http://localhost:8080 (ver docs/CREDENCIALES_UI.md)"
}

_start_api() {
  export API_SMART_GRID_PORT="${API_SMART_GRID_PORT:-8000}"
  local port="$API_SMART_GRID_PORT"
  if _port_open "$port"; then
    echo "=== API Swagger: ya responde en $port ==="
    return 0
  fi
  local script="$BASE/scripts/iniciar_api_smart_grid.sh"
  if [[ ! -x "$script" ]]; then
    echo "AVISO: No encuentro $script. pip install fastapi uvicorn" >&2
    return 1
  fi
  echo "=== API Swagger: arrancando (puerto $port) ==="
  local logf="/tmp/smart_grid_api.log"
  nohup bash "$script" >>"$logf" 2>&1 &
  echo "  pid=$! · log: $logf"
  echo "  Swagger UI: http://localhost:$port/docs"
}

if _should_run hdfs; then
  _start_hdfs
fi
if _should_run kafka; then
  _start_kafka
fi
if _should_run cassandra; then
  _start_cassandra
fi
if _should_run airflow; then
  _start_airflow
fi
if _should_run api; then
  _start_api
fi

# Calentar catálogo Hive en background (JVM/metastore) para que el dashboard no falle por timeout
_warm_hive_catalog() {
  local exe=""
  for cand in "$HOME/apache-hive-4.2.0-bin/bin/hive" "$HOME/apache-hive-4.0.0-bin/bin/hive" "/opt/hive/bin/hive" "${HIVE_HOME:-}/bin/hive"; do
    [[ -x "${cand:-}" ]] && exe="$cand" && break
  done
  [[ -z "$exe" ]] && exe="$(command -v hive 2>/dev/null)" || true
  [[ -z "$exe" ]] && exe="$(command -v spark-sql 2>/dev/null)" || true
  [[ -z "$exe" ]] && exe="/opt/spark/bin/spark-sql"
  if [[ -x "$exe" ]]; then
    echo "=== Hive/Spark: calentando catálogo en background (evita timeouts en dashboard) ==="
    nohup "$exe" -e "SHOW DATABASES;" >>/tmp/smart_grid_hive_warmup.log 2>&1 &
    echo "  pid=$! · log: /tmp/smart_grid_hive_warmup.log"
  fi
}

if [[ -z "$ONLY" ]]; then
  _warm_hive_catalog
fi

echo ""
echo "Directorio de trabajo: $BASE"
echo "Esquema Cassandra (cuando 9042 responda): cqlsh -f cassandra/esquema_smart_grid.cql"
echo "Airflow UI: http://localhost:8080 (ver docs/CREDENCIALES_UI.md)"
echo "API Swagger: http://localhost:${API_SMART_GRID_PORT:-8000}/docs"
echo ""
echo "Para tener cqlsh, hive, spark-sql en el PATH: source $BASE/scripts/env_smart_grid.sh"
