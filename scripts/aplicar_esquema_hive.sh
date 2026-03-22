#!/usr/bin/bash
# Aplica setup_hive.hql con spark-sql (o hive) usando warehouse en HDFS.
# Evita warnings "Location: file://... specified for non-external table".
#
# Requiere: HDFS arrancado, spark-sql o hive en PATH.
# Uso: cd ~/smart_energy && ./scripts/aplicar_esquema_hive.sh
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HQL="${BASE}/setup_hive.hql"
HDFS_FS="${HDFS_DEFAULT_FS:-hdfs://localhost:9000}"
WAREHOUSE="${HDFS_FS%/}/user/hive/warehouse"

if [[ ! -f "$HQL" ]]; then
  echo "ERROR: No existe $HQL"
  exit 1
fi

# Crear directorio warehouse en HDFS si no existe
if command -v hdfs &>/dev/null; then
  hdfs dfs -mkdir -p /user/hive/warehouse 2>/dev/null || true
fi

exe=""
for cand in /opt/spark/bin/spark-sql "$HOME/apache-hive-4.2.0-bin/bin/hive" "$HOME/apache-hive-4.0.0-bin/bin/hive"; do
  [[ -x "$cand" ]] && exe="$cand" && break
done
[[ -z "$exe" ]] && exe="$(command -v spark-sql 2>/dev/null)" || true
[[ -z "$exe" ]] && exe="$(command -v hive 2>/dev/null)" || true

if [[ -z "$exe" ]]; then
  echo "ERROR: No se encontró spark-sql ni hive. Instala: ./scripts/instalar_hive_java21.sh" >&2
  exit 1
fi

echo "Aplicando $HQL con $exe (warehouse=$WAREHOUSE)"
if [[ "$(basename "$exe")" == "spark-sql" ]]; then
  exec "$exe" --conf "spark.sql.warehouse.dir=$WAREHOUSE" -f "$HQL"
else
  exec "$exe" -f "$HQL"
fi
