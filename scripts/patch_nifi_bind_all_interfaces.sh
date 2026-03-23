#!/usr/bin/bash
# NiFi por defecto enlaza a 127.0.0.1; el enlace del dashboard puede usar la IP
# de la interfaz (ej. 10.0.2.15 en VM) y fallar. Este script configura
# nifi.web.https.host=0.0.0.0 para que NiFi escuche en todas las interfaces.
#
# Uso: ./scripts/patch_nifi_bind_all_interfaces.sh
# Luego: reinicia NiFi (nifi.sh stop; nifi.sh start)
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NIFI_HOME="${NIFI_HOME:-$BASE/nifi-2.6.0}"
CONF="${NIFI_HOME}/conf/nifi.properties"

if [[ ! -f "$CONF" ]]; then
  echo "ERROR: No existe $CONF"
  echo "Instala NiFi: ./scripts/instalar_nifi_260.sh"
  exit 1
fi

if grep -q '^nifi.web.https.host=0.0.0.0' "$CONF" 2>/dev/null; then
  echo "Ya configurado: nifi.web.https.host=0.0.0.0"
  exit 0
fi

# Crear backup
cp -a "$CONF" "${CONF}.bak.$(date +%Y%m%d%H%M%S)"

# Añadir o reemplazar la propiedad
if grep -q '^nifi.web.https.host=' "$CONF" 2>/dev/null; then
  sed -i 's/^nifi.web.https.host=.*/nifi.web.https.host=0.0.0.0/' "$CONF"
else
  echo "" >> "$CONF"
  echo "# Smart Grid: escuchar en todas las interfaces (patch_nifi_bind_all_interfaces.sh)" >> "$CONF"
  echo "nifi.web.https.host=0.0.0.0" >> "$CONF"
fi

echo "Parche aplicado. Reinicia NiFi:"
echo "  $NIFI_HOME/bin/nifi.sh stop"
echo "  $NIFI_HOME/bin/nifi.sh start"
echo ""
echo "Tras reiniciar, https://<IP_SERVIDOR>:8443/nifi será accesible."
echo ""
