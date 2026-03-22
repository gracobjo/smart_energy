#!/usr/bin/bash
# Comprueba estado de servicios Smart Grid.
# Exit 0 si todos OK; exit 1 si alguno falla.
# Uso: ./scripts/comprobar_servicios.sh
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FAIL=0

_port_ok() {
  local p="$1"
  (command -v nc &>/dev/null && nc -z -w1 127.0.0.1 "$p") 2>/dev/null && return 0
  return 1
}

echo "=== Comprobación servicios Smart Grid ==="
echo ""

if _port_ok 9870 || _port_ok 9000; then
  echo "HDFS: OK (puerto activo)"
  hdfs dfs -ls / &>/dev/null && echo "  HDFS CLI: OK" || echo "  HDFS CLI: fallo"
else
  echo "HDFS: NO (puertos 9870/9000 cerrados)"
  FAIL=1
fi

if _port_ok 9092; then
  echo "Kafka: OK (9092)"
else
  echo "Kafka: NO (9092 cerrado)"
  FAIL=1
fi

if _port_ok 9042; then
  echo "Cassandra: OK (9042)"
else
  echo "Cassandra: NO (9042 cerrado)"
  FAIL=1
fi

if _port_ok 8443; then
  echo "NiFi: OK (8443)"
else
  echo "NiFi: NO (8443 cerrado)"
fi

if _port_ok 8080; then
  echo "Airflow: OK (8080)"
else
  echo "Airflow: NO (8080 cerrado)"
fi

echo ""
[[ $FAIL -eq 0 ]] && echo "Servicios base: OK" || { echo "Algunos servicios no están activos."; exit 1; }
