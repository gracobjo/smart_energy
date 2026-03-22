#!/usr/bin/bash
# Corrige: "Database at ~/.hive_metastore/metastore_db has an incompatible format
# with the current version of the software. The database was created by version 10.17."
#
# Causa: El metastore Derby embebido se creó con otra versión de Derby/Spark/Hive.
# Solución: respaldar y eliminar el metastore antiguo; Hive/Spark creará uno nuevo.
#
# Uso:
#   ./scripts/fix_hive_metastore_derby_incompatible.sh
set -euo pipefail

METASTORE_DIR="${HOME}/.hive_metastore"

if [ ! -d "$METASTORE_DIR" ]; then
  echo "No existe $METASTORE_DIR. No hay nada que corregir."
  exit 0
fi

BACKUP="${METASTORE_DIR}.bak.$(date +%Y%m%d_%H%M%S)"
echo "Respaldando $METASTORE_DIR -> $BACKUP"
mv "$METASTORE_DIR" "$BACKUP"
echo ""
echo "Listo. El metastore antiguo se respaldó en:"
echo "  $BACKUP"
echo ""
echo "Siguiente paso: vuelve a crear el esquema Hive (crea base y tablas):"
echo "  cd ~/smart_energy"
echo "  ./scripts/aplicar_esquema_hive.sh"
echo "  # o desde el dashboard: Fase 0 → Arrancar"
echo ""
