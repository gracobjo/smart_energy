#!/usr/bin/bash
# Aplica cassandra/esquema_smart_grid.cql usando el cqlsh empaquetado en el proyecto
# (no hace falta tener `cqlsh` en el PATH global).
#
# Si ves ModuleNotFoundError: No module named 'six.moves', usa ./scripts/cqlsh_local.sh
# o: python3 -m pip install --user six && export CQLSH_PYTHON="$(command -v python3)"
#
# Uso: cd ~/smart_energy && ./scripts/aplicar_esquema_cassandra.sh
set -euo pipefail
BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CQL="${BASE}/cassandra/esquema_smart_grid.cql"
WRAPPER="${BASE}/scripts/cqlsh_local.sh"
if [[ ! -f "$CQL" ]]; then
  echo "ERROR: No existe $CQL"
  exit 1
fi
if [[ ! -x "$WRAPPER" ]]; then
  echo "ERROR: No ejecutable: $WRAPPER"
  exit 1
fi
# Misma JVM que Cassandra (17) si existe (por si algún subproceso la consulta)
for _jdk in /usr/lib/jvm/java-17-openjdk-amd64 /usr/lib/jvm/java-11-openjdk-amd64; do
  if [[ -x "${_jdk}/bin/java" ]]; then
    export JAVA_HOME="${_jdk}"
    break
  fi
done
echo "Aplicando esquema con: $WRAPPER"
exec "$WRAPPER" -f "$CQL"
