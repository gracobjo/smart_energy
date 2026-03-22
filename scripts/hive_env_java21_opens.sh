#!/usr/bin/bash
# Añade a hive-env.sh los --add-opens necesarios para Java 21 con Hive 4.x (CliDriver).
# Sin ellos: InaccessibleObjectException (java.net.URI.string en StringInternUtils).
#
# Uso:
#   export HIVE_HOME=~/apache-hive-4.2.0-bin
#   ./scripts/hive_env_java21_opens.sh
set -euo pipefail

HIVE_HOME="${HIVE_HOME:-$HOME/apache-hive-4.2.0-bin}"
ENV_SH="${HIVE_HOME}/conf/hive-env.sh"

if [ ! -f "$ENV_SH" ]; then
  echo "ERROR: No existe $ENV_SH"
  exit 1
fi

if grep -q 'smart_energy: Java 21 --add-opens' "$ENV_SH" 2>/dev/null; then
  echo "Ya está aplicado en $ENV_SH"
  exit 0
fi

cat >> "$ENV_SH" << 'ENVEOF'

# --- smart_energy: Java 21 --add-opens (Hive CliDriver / StringInternUtils) ---
HIVE_JAVA21_OPENS="--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED"
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} ${HIVE_JAVA21_OPENS}"
# --- fin Java 21 opens ---
ENVEOF

echo "Añadido HADOOP_CLIENT_OPTS (--add-opens) en $ENV_SH"
echo "Prueba: USE_BEELINE_FOR_HIVE_CLI=false hive -e \"SHOW DATABASES;\""
