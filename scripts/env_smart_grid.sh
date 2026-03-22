#!/usr/bin/bash
# Añade Cassandra, Hive, Spark, etc. al PATH para la sesión actual.
# Uso: source ./scripts/env_smart_grid.sh
# O en ~/.bashrc: source ~/smart_energy/scripts/env_smart_grid.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE="$(cd "$SCRIPT_DIR/.." && pwd)"

# Cassandra (cqlsh, nodetool)
if [[ -d "$BASE/cassandra/bin" ]]; then
  export PATH="$BASE/cassandra/bin:$PATH"
fi

# Hive
for h in "$BASE/apache-hive-4.2.0-bin" "$BASE/apache-hive-4.0.0-bin" "${HIVE_HOME:-}" "/opt/hive"; do
  [[ -d "${h}/bin" ]] && export PATH="${h}/bin:$PATH" && break
done

# Spark
for s in "$BASE/spark" "${SPARK_HOME:-}" "/opt/spark"; do
  [[ -d "${s}/bin" ]] && export SPARK_HOME="$s" && export PATH="${s}/bin:$PATH" && break
done

# Hadoop (opcional)
for h in "${HADOOP_HOME:-}" "/opt/hadoop"; do
  [[ -d "${h}/bin" ]] && export HADOOP_HOME="$h" && export PATH="${h}/bin:${h}/sbin:$PATH" && break
done

# Kafka (opcional)
for k in "${KAFKA_HOME:-}" "/opt/kafka" "$BASE/kafka"; do
  [[ -d "${k}/bin" ]] && export KAFKA_HOME="$k" && export PATH="${k}/bin:$PATH" && break
done

# NiFi 2.6.0 (opcional)
for n in "$BASE/nifi-2.6.0" "${NIFI_HOME:-}"; do
  [[ -d "${n}/bin" ]] && [[ -x "${n}/bin/nifi.sh" ]] && export NIFI_HOME="$n" && export PATH="${n}/bin:$PATH" && break
done

echo "Entorno Smart Grid cargado (cqlsh, hive, spark-sql, nifi, etc. en PATH)"
