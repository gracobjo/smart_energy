#!/usr/bin/bash
# cqlsh empaquetado en Cassandra 4.x incluye six-1.12 en cassandra/lib/*.zip.
# Con Python 3.12+ eso provoca: ModuleNotFoundError: No module named 'six.moves'
# porque ese six antiguo se antepone en sys.path al del sistema.
#
# Este script sustituye el zip por six 1.16 (wheel de PyPI renombrado a .zip).
#
# Uso: desde la raíz del repo:  ./scripts/fix_cqlsh_six_python312.sh
set -euo pipefail
BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LIB="${BASE}/cassandra/lib"
if [[ ! -d "$LIB" ]]; then
  echo "ERROR: No existe $LIB (¿Cassandra descomprimido en cassandra/?)"
  exit 1
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
python3 -m pip download --no-deps -d "$TMP" 'six==1.16.0'
WHL="$(echo "$TMP"/six-1.16.0-py2.py3-none-any.whl)"
if [[ ! -f "$WHL" ]]; then
  echo "ERROR: No se descargó six wheel"
  exit 1
fi

# Quitar six embebido antiguo (1.12 u otro); se reemplaza por 1.16
shopt -s nullglob
for old in "$LIB"/six-*.zip; do
  echo "Eliminando: $old"
  rm -f "$old"
done
shopt -u nullglob

DEST="${LIB}/six-1.16.0-py2.py3-none-any.zip"
cp "$WHL" "$DEST"
echo "Instalado: $DEST"

if ! python3 -c "
import sys
sys.path.insert(0, '$DEST')
import six.moves
print('OK: six.moves con zip embebido')
"; then
  echo "ERROR: comprobación de six.moves falló"
  exit 1
fi

echo "Listo. Prueba: ./cassandra/bin/cqlsh --help"
