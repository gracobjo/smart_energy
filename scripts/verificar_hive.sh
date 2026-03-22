#!/usr/bin/bash
# Comprueba si Apache Hive está disponible y muestra cómo probarlo.
set -e
echo "=== Verificación Hive (Smart Grid) ==="

HIVE_CANDIDATES=(
  "${HIVE_HOME:-}"
  "$HOME/apache-hive-4.2.0-bin"
  "$HOME/apache-hive-4.0.0-bin"
  "/opt/hive"
  "$HOME/hive"
  "$HOME/apache-hive"
  "/home/hadoop/proyecto_transporte_global/hive"
)

HIVE_BIN=""
for d in "${HIVE_CANDIDATES[@]}"; do
  [ -z "$d" ] && continue
  if [ -x "$d/bin/hive" ]; then
    HIVE_BIN="$d/bin/hive"
    export HIVE_HOME="$d"
    echo "Encontrado: HIVE_HOME=$d"
    break
  fi
done

if [ -z "$HIVE_BIN" ]; then
  echo "No se encontró Hive en rutas habituales."
  echo "Para Java 17/21 instala Hive 4.x:"
  echo "  ./scripts/instalar_hive_java21.sh"
  exit 1
fi

echo "Cliente hive: $HIVE_BIN"
if command -v beeline &>/dev/null; then
  echo "Beeline: $(command -v beeline)"
fi

echo ""
if [ -n "${JAVA_HOME:-}" ] && [ -x "${JAVA_HOME}/bin/java" ]; then
  echo "JAVA_HOME=$JAVA_HOME"
  echo "  $($JAVA_HOME/bin/java -version 2>&1 | head -1)"
else
  echo "AVISO: JAVA_HOME no apunta a un JDK (Hive 4.2 necesita Java 21)."
fi
if [ -n "${JAVA_HOME:-}" ] && ! "${JAVA_HOME}/bin/java" -version 2>&1 | grep -qE ' version "21\.|openjdk version "21'; then
  echo "ERROR: Hive 4.2 requiere Java 21 en JAVA_HOME (no Java 17)."
  echo "  Ej.: export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64"
  echo "  export PATH=\$JAVA_HOME/bin:\$PATH"
fi

echo ""
echo "Prueba rápida (metastore local / ya configurado):"
echo "  export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64   # u otra ruta JDK 21"
echo "  export PATH=\$JAVA_HOME/bin:\$PATH"
echo "  export HIVE_HOME=$HIVE_HOME"
echo "  export PATH=\$HIVE_HOME/bin:\$PATH"
echo "  hive -e 'SHOW DATABASES;'"
echo ""
echo "Si usas JDBC (HiveServer2 en 10000):"
echo "  beeline -u 'jdbc:hive2://localhost:10000' -e 'SHOW DATABASES;'"
exit 0
