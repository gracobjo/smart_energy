#!/usr/bin/bash
# Verifica que NiFi 2.6.0 está instalado y opcionalmente arrancado.
# Uso: ./scripts/verificar_nifi.sh
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NIFI_HOME="${NIFI_HOME:-$BASE/nifi-2.6.0}"

echo "=== Verificación NiFi 2.6.0 ==="
if [[ ! -d "$NIFI_HOME" ]]; then
  echo "ERROR: No existe $NIFI_HOME"
  echo "Instala con: ./scripts/instalar_nifi_260.sh"
  exit 1
fi

if [[ ! -x "$NIFI_HOME/bin/nifi.sh" ]]; then
  echo "ERROR: No ejecutable $NIFI_HOME/bin/nifi.sh"
  exit 1
fi

echo "NiFi instalado en: $NIFI_HOME"
"$NIFI_HOME/bin/nifi.sh" status 2>/dev/null || true

if command -v nc &>/dev/null; then
  if nc -z -w1 127.0.0.1 8443 2>/dev/null; then
    echo "UI disponible: https://localhost:8443/nifi"
  else
    echo "Puerto 8443 cerrado. Arranca con: $NIFI_HOME/bin/nifi.sh start"
  fi
fi
echo ""
echo "Dashboard: Fase 1 → NiFi → Arrancar NiFi"
echo ""
