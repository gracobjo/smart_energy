#!/usr/bin/bash
# Muestra qué JVM usa realmente Hadoop (origen típico del error
# UnsupportedClassVersionError 61 vs 65 al ejecutar schematool/hive).
#
# Uso:
#   chmod +x scripts/diagnostico_java_hadoop_hive.sh
#   ./scripts/diagnostico_java_hadoop_hive.sh
set -euo pipefail

HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
HIVE_HOME="${HIVE_HOME:-$HOME/apache-hive-4.2.0-bin}"
HENV="${HADOOP_HOME}/etc/hadoop/hadoop-env.sh"

echo "=== 1) java en tu shell actual ==="
command -v java 2>/dev/null || echo "(no hay java en PATH)"
java -version 2>&1 | head -1 || true
echo "JAVA_HOME=${JAVA_HOME:-<no definido>}"

echo ""
echo "=== 2) Líneas JAVA_HOME en hadoop-env.sh (suele pisar tu export) ==="
if [ -f "$HENV" ]; then
  echo "Archivo: $HENV"
  grep -nE 'JAVA_HOME' "$HENV" || echo "(ninguna coincidencia; revisa a mano el fichero)"
else
  echo "No existe $HENV — ajusta HADOOP_HOME"
fi

echo ""
echo "=== 3) hive-env.sh (Hive) ==="
if [ -f "${HIVE_HOME}/conf/hive-env.sh" ]; then
  grep -nE 'JAVA_HOME' "${HIVE_HOME}/conf/hive-env.sh" || echo "(sin JAVA_HOME en hive-env.sh)"
else
  echo "No existe ${HIVE_HOME}/conf/hive-env.sh"
fi

echo ""
echo "=== 4) JVM tras simular solo Hadoop (como hace bin/hadoop al arrancar) ==="
if [ -f "$HENV" ]; then
  # Sin variables del usuario: muchos clusters solo definen JAVA_HOME en hadoop-env.sh
  env -i HOME="$HOME" USER="${USER:-hadoop}" PATH="/usr/bin:/bin" bash --norc --noprofile -c "
    export HADOOP_HOME='${HADOOP_HOME}'
    export HADOOP_CONF_DIR='${HADOOP_HOME}/etc/hadoop'
    set -a
    # shellcheck disable=SC1090
    source '${HENV}' 2>/dev/null || true
    set +a
    if [ -n \"\${JAVA_HOME:-}\" ] && [ -x \"\${JAVA_HOME}/bin/java\" ]; then
      echo \"JAVA_HOME efectivo (hadoop-env): \$JAVA_HOME\"
      \"\$JAVA_HOME/bin/java\" -version 2>&1 | head -1
    else
      echo 'JAVA_HOME no quedó definido tras source (raro).'
    fi
  "
else
  echo "(omitido: sin hadoop-env.sh)"
fi

echo ""
echo "=== Qué hacer ==="
echo "bin/schematool y bin/hive llaman a \$HADOOP_HOME/bin/hadoop, que ejecuta:"
echo "  source \$HADOOP_HOME/etc/hadoop/hadoop-env.sh"
echo "Ese fichero suele fijar JAVA_HOME a Java 17 u 11, y anula el export de tu terminal."
echo ""
echo "Edita (sudo si hace falta) y deja Java 21, por ejemplo:"
echo "  export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64"
echo "en: $HENV"
echo ""
echo "Tras guardar, vuelve a: cd ~/apache-hive-4.2.0-bin && bin/schematool -dbType derby -initSchema"
