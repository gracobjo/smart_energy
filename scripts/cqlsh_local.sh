#!/usr/bin/bash
# Ejecuta el cqlsh empaquetado en cassandra/bin/ con un Python que tenga `six`.
# Si el error es `six.moves` con Python 3.12+, suele ser el zip viejo en
# cassandra/lib/six-1.12*.zip → ejecuta ./scripts/fix_cqlsh_six_python312.sh
#
# Uso (desde la raíz del repo):
#   ./scripts/cqlsh_local.sh -f cassandra/esquema_smart_grid.cql
#   ./scripts/cqlsh_local.sh 127.0.0.1 9042 -e "DESCRIBE KEYSPACES;"
#
# Opcional: export CQLSH_PYTHON=/ruta/a/python3
set -euo pipefail
BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CQLSH="${BASE}/cassandra/bin/cqlsh"
if [[ ! -x "$CQLSH" ]]; then
  echo "ERROR: No ejecutable: $CQLSH"
  exit 1
fi

_py="${CQLSH_PYTHON:-python3}"
if ! "$_py" -c "import six" 2>/dev/null; then
  echo "Instalando paquete Python 'six' (requerido por cqlsh)..."
  "$_py" -m pip install --user -q six || {
    echo "ERROR: pip install six falló. Prueba: sudo apt install python3-six"
    exit 1
  }
fi
export CQLSH_PYTHON="$_py"
exec "$CQLSH" "$@"
