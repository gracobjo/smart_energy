#!/usr/bin/bash
# Comenta -XX:-UseBiasedLocking en jvm-server.options (Java 21 no reconoce el flag).
# El bloque JAVA_HOME/JAVA en cassandra-env.sh va en el repo; si falta, copia desde README_DESPLIEGUE.
#
# Uso: ./scripts/patch_cassandra_java21_jvm.sh
set -euo pipefail
BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JVM_OPTS="${BASE}/cassandra/conf/jvm-server.options"
if [[ ! -f "$JVM_OPTS" ]]; then
  echo "ERROR: No existe $JVM_OPTS"
  exit 1
fi
if grep -q '^#-XX:-UseBiasedLocking' "$JVM_OPTS" 2>/dev/null && ! grep -q '^-XX:-UseBiasedLocking$' "$JVM_OPTS" 2>/dev/null; then
  echo "UseBiasedLocking ya comentado."
  exit 0
fi
if grep -q '^-XX:-UseBiasedLocking$' "$JVM_OPTS" 2>/dev/null; then
  cp -a "$JVM_OPTS" "${JVM_OPTS}.bak.$(date +%Y%m%d%H%M%S)"
  sed -i 's/^-XX:-UseBiasedLocking$/#-XX:-UseBiasedLocking  # Java 21+/' "$JVM_OPTS"
  echo "Parche aplicado en jvm-server.options."
else
  echo "No hay línea activa UseBiasedLocking."
fi
