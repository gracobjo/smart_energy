#!/usr/bin/bash
# Instala Apache NiFi 2.6.0 en el directorio del proyecto.
# Requiere: curl o wget, unzip, Java 17+
#
# Uso: cd ~/smart_energy && ./scripts/instalar_nifi_260.sh
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$BASE"
TARGET="${NIFI_HOME:-$BASE/nifi-2.6.0}"
NIFI_VER="2.6.0"
URL="https://archive.apache.org/dist/nifi/${NIFI_VER}/nifi-${NIFI_VER}-bin.zip"
ZIP="$BASE/nifi-${NIFI_VER}-bin.zip"

if [[ -d "$TARGET" ]] && [[ -x "$TARGET/bin/nifi.sh" ]]; then
  echo "NiFi ${NIFI_VER} ya instalado en: $TARGET"
  echo "Para reinstalar, borra el directorio y ejecuta de nuevo."
  exit 0
fi

echo "=== Instalando Apache NiFi ${NIFI_VER} ==="
echo "Destino: $TARGET"
echo ""

if [[ ! -f "$ZIP" ]]; then
  echo "Descargando $URL ..."
  if command -v curl &>/dev/null; then
    curl -sL -o "$ZIP" "$URL"
  elif command -v wget &>/dev/null; then
    wget -q -O "$ZIP" "$URL"
  else
    echo "ERROR: Necesitas curl o wget para descargar."
    exit 1
  fi
fi

echo "Descomprimiendo..."
rm -rf "$BASE/nifi-${NIFI_VER}"
unzip -q -o "$ZIP" -d "$BASE"
mv "$BASE/nifi-${NIFI_VER}" "$TARGET"

# Crear directorio para logs GPS (origen para GetFile)
mkdir -p "$BASE/data/gps_logs"
echo '{"gps_logs":[{"lat":40.42,"lon":-3.70,"timestamp":"2025-01-01T12:00:00","vehiculo":"mantenimiento_01"}],"fuente":"nifi"}' \
  > "$BASE/data/gps_logs/ejemplo_gps.json"

echo ""
echo "=== NiFi ${NIFI_VER} instalado en: $TARGET ==="
echo ""
echo "Variables recomendadas (.bashrc o .env):"
echo "  export NIFI_HOME=$TARGET"
echo "  export PATH=\$NIFI_HOME/bin:\$PATH"
echo ""
echo "Arrancar: \$NIFI_HOME/bin/nifi.sh start"
echo "UI: https://localhost:8443/nifi (puede tardar 1–2 min en abrir)"
echo ""
echo "Desde el dashboard: Fase 1 → NiFi → Arrancar NiFi"
echo ""
