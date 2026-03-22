#!/usr/bin/bash
# Para servicios base Smart Grid.
# Uso: ./scripts/parar_servicios.sh [--only hdfs|kafka|cassandra|nifi|airflow]
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NIFI_HOME="${NIFI_HOME:-$BASE/nifi-2.6.0}"
HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"

ONLY=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --only) shift; ONLY="${1:-}"; shift || true ;;
    -h|--help) echo "Uso: $0 [--only hdfs|kafka|cassandra|nifi|airflow]"; exit 0 ;;
    *) echo "Opción desconocida: $1" >&2; exit 1 ;;
  esac
done

_should_run() { [[ -z "$ONLY" ]] || [[ "$ONLY" == "$1" ]]; }

_stop_hdfs() {
  if _should_run hdfs; then
    echo "=== Parando HDFS ==="
    if [[ -x "$HADOOP_HOME/sbin/stop-all.sh" ]]; then
      "$HADOOP_HOME/sbin/stop-all.sh" 2>/dev/null || "$HADOOP_HOME/sbin/stop-dfs.sh" 2>/dev/null || true
    else
      stop-all.sh 2>/dev/null || stop-dfs.sh 2>/dev/null || true
    fi
    echo "HDFS parado."
  fi
}

_stop_kafka() {
  if _should_run kafka; then
    echo "=== Parando Kafka ==="
    pkill -f "kafka.Kafka" 2>/dev/null || pkill -f "kafka-server-start" 2>/dev/null || true
    echo "Kafka parado (pkill)."
  fi
}

_stop_cassandra() {
  if _should_run cassandra; then
    echo "=== Parando Cassandra ==="
    pkill -f "cassandra/bin/cassandra" 2>/dev/null || pkill -f "org.apache.cassandra.service.CassandraDaemon" 2>/dev/null || true
    echo "Cassandra parado (pkill)."
  fi
}

_stop_nifi() {
  if _should_run nifi; then
    echo "=== Parando NiFi ==="
    if [[ -x "$NIFI_HOME/bin/nifi.sh" ]]; then
      "$NIFI_HOME/bin/nifi.sh" stop 2>/dev/null || true
      echo "NiFi parado."
    else
      echo "AVISO: NiFi no encontrado en $NIFI_HOME"
    fi
  fi
}

_stop_airflow() {
  if _should_run airflow; then
    echo "=== Parando Airflow ==="
    pkill -f "airflow api-server" 2>/dev/null || true
    pkill -f "airflow scheduler" 2>/dev/null || true
    pkill -f "airflow webserver" 2>/dev/null || true
    echo "Airflow parado (api-server, scheduler, webserver)."
  fi
}

_stop_hdfs
_stop_kafka
_stop_cassandra
_stop_nifi
_stop_airflow
echo ""
echo "Servicios parados."
