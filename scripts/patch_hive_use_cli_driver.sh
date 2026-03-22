#!/usr/bin/bash
# Hive 4.x fuerza USE_BEELINE_FOR_HIVE_CLI=true en bin/hive antes de cargar hive-env.sh.
# Entonces `hive -e "SQL"` usa Beeline sin URL → "No current connection".
#
# Este parche hace que se respete la variable de entorno:
#   export USE_BEELINE_FOR_HIVE_CLI=false
# y se use el CLI clásico (CliDriver) contra el metastore local (Derby embebido).
#
# Uso:
#   export HIVE_HOME=~/apache-hive-4.2.0-bin
#   ./scripts/patch_hive_use_cli_driver.sh
#   export USE_BEELINE_FOR_HIVE_CLI=false
#   hive -e "SHOW DATABASES;"
set -euo pipefail

HIVE_HOME="${HIVE_HOME:-$HOME/apache-hive-4.2.0-bin}"
HIVE_BIN="${HIVE_HOME}/bin/hive"

if [ ! -f "$HIVE_BIN" ]; then
  echo "ERROR: No existe $HIVE_BIN"
  exit 1
fi

if grep -q 'USE_BEELINE_FOR_HIVE_CLI="\${USE_BEELINE_FOR_HIVE_CLI:-true}"' "$HIVE_BIN" 2>/dev/null; then
  echo "Ya está parcheado: $HIVE_BIN"
  exit 0
fi

cp -a "$HIVE_BIN" "${HIVE_BIN}.bak.$(date +%Y%m%d%H%M%S)"
# Línea que fija true → respeta $USE_BEELINE_FOR_HIVE_CLI
sed -i 's/^USE_BEELINE_FOR_HIVE_CLI="true"$/USE_BEELINE_FOR_HIVE_CLI="${USE_BEELINE_FOR_HIVE_CLI:-true}"/' "$HIVE_BIN"

echo "Parche aplicado (backup: ${HIVE_BIN}.bak.*)"
echo ""
echo "Añade a ~/.bashrc o ejecuta antes de hive -e:"
echo "  export USE_BEELINE_FOR_HIVE_CLI=false"
echo "  export HIVE_HOME=$HIVE_HOME"
echo "  export PATH=\"\$HIVE_HOME/bin:\$PATH\""
