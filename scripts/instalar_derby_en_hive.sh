#!/usr/bin/bash
# Apache Hive 4.2.x: metastore Derby embebido.
#
# 1) Descarga derby-*.jar a $HIVE_HOME/lib/ si faltan.
# 2) Añade Derby a HADOOP_CLASSPATH en hive-env.sh — sin esto, schematool usa
#    "hadoop jar" con un classpath que NO incluye todo lib/, y falla con:
#    ClassNotFoundException: org.apache.derby.jdbc.EmbeddedDriver
#    aunque los JARs existan en lib/.
#
# Uso:
#   export HIVE_HOME=~/apache-hive-4.2.0-bin
#   ./scripts/instalar_derby_en_hive.sh
set -euo pipefail

HIVE_HOME="${HIVE_HOME:-$HOME/apache-hive-4.2.0-bin}"
DERBY_VER="${DERBY_VERSION:-10.15.2.0}"
DERBY_JAR="derby-${DERBY_VER}.jar"
LIB="${HIVE_HOME}/lib"
URL="https://repo1.maven.org/maven2/org/apache/derby/derby/${DERBY_VER}/${DERBY_JAR}"
ENV_SH="${HIVE_HOME}/conf/hive-env.sh"

_append_hadoop_classpath_derby() {
  if [ ! -f "$ENV_SH" ]; then
    echo "# Generado por instalar_derby_en_hive.sh" > "$ENV_SH"
  fi
  if grep -q 'smart_energy: Derby → HADOOP_CLASSPATH' "$ENV_SH" 2>/dev/null; then
    echo "hive-env.sh ya contiene el bloque HADOOP_CLASSPATH (Derby)."
    return 0
  fi
  cat >> "$ENV_SH" << 'ENVEOF'

# --- smart_energy: Derby → HADOOP_CLASSPATH (schematool / hadoop jar) ---
# Sin esto, los derby*.jar en lib/ no entran en el classpath y falla EmbeddedDriver.
if [ -n "${HIVE_HOME:-}" ] && [ -d "${HIVE_HOME}/lib" ]; then
  _derby_cp=""
  for _j in "${HIVE_HOME}/lib"/derby*.jar; do
    [ -f "$_j" ] || continue
    _derby_cp="${_derby_cp:+${_derby_cp}:}$_j"
  done
  if [ -n "$_derby_cp" ]; then
    export HADOOP_CLASSPATH="${_derby_cp}${HADOOP_CLASSPATH:+:${HADOOP_CLASSPATH}}"
  fi
fi
# --- fin Derby ---
ENVEOF
  echo "Añadido bloque Derby en HADOOP_CLASSPATH -> $ENV_SH"
}

if [ ! -d "$LIB" ]; then
  echo "ERROR: No existe $LIB — define HIVE_HOME correctamente."
  exit 1
fi

if ! find "$LIB" -maxdepth 1 -name 'derby*.jar' 2>/dev/null | grep -q .; then
  echo "Descargando ${DERBY_JAR} -> ${LIB}/"
  curl -fSL -o "${LIB}/${DERBY_JAR}" "$URL"
  echo "OK: ${LIB}/${DERBY_JAR}"
else
  echo "OK: ya hay JARs Derby en $LIB:"
  find "$LIB" -maxdepth 1 -name 'derby*.jar' -print
fi

_append_hadoop_classpath_derby

echo ""
echo "Si schematool sigue fallando con EmbeddedDriver:"
echo "  ./scripts/fix_hive_schematool_derby.sh   # copia derby a Hadoop common/lib"
echo ""
echo "Siguiente paso:"
echo "  export HIVE_HOME=\"$HIVE_HOME\""
echo "  cd \"$HIVE_HOME\""
echo "  bin/schematool -dbType derby -initSchema"
