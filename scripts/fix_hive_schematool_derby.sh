#!/usr/bin/bash
# Workaround cuando HADOOP_CLASSPATH en hive-env.sh no basta:
# copia derby*.jar a \$HADOOP_HOME/share/hadoop/common/lib/
# para que hadoop jar los incluya en el classpath por defecto.
#
# Si tienes varias instalaciones de Hive, fija HIVE_HOME al correcto antes de ejecutar.
#
# Uso:
#   export HIVE_HOME=~/apache-hive-4.2.0-bin   # imprescindible si tienes otro hive
#   ./scripts/fix_hive_schematool_derby.sh
set -euo pipefail

HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
HIVE_HOME="${HIVE_HOME:-$HOME/apache-hive-4.2.0-bin}"
HADOOP_LIB="${HADOOP_HOME}/share/hadoop/common/lib"

if [ ! -d "$HADOOP_LIB" ]; then
  echo "ERROR: No existe $HADOOP_LIB"
  exit 1
fi

count=0
for j in "${HIVE_HOME}/lib"/derby*.jar; do
  [ -f "$j" ] || continue
  name=$(basename "$j")
  if [ ! -f "${HADOOP_LIB}/${name}" ] || [ "$j" -nt "${HADOOP_LIB}/${name}" ]; then
    echo "Copiando $j -> $HADOOP_LIB/"
    cp -p "$j" "${HADOOP_LIB}/"
    ((count++)) || true
  fi
done

if [ "$count" -eq 0 ]; then
  echo "Derby ya presente en $HADOOP_LIB (o no hay derby*.jar en $HIVE_HOME/lib)"
else
  echo "Listo: $count JAR(s) copiado(s)."
fi

echo ""
echo "Siguiente paso:"
echo "  cd ~/apache-hive-4.2.0-bin"
echo "  bin/schematool -dbType derby -initSchema"
