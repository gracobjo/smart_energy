#!/usr/bin/bash
# Instala Apache Hive 4.2.x (compatible con OpenJDK 21).
# Hive 3.1.x falla con Java 9+ (ClassCastException URLClassLoader).
#
# Uso:
#   chmod +x scripts/instalar_hive_java21.sh
#   ./scripts/instalar_hive_java21.sh
#
# Tras instalar (añade a ~/.bashrc):
#   export HIVE_HOME=$HOME/apache-hive-4.2.0-bin
#   export PATH=$HIVE_HOME/bin:$PATH
#   export HADOOP_HOME=/opt/hadoop   # o tu ruta
set -euo pipefail

HIVE_VERSION="${HIVE_VERSION:-4.2.0}"
HIVE_PKG="apache-hive-${HIVE_VERSION}-bin"
TGZ="${HIVE_PKG}.tar.gz"
BASE_URL="https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}"
INSTALL_ROOT="${HIVE_INSTALL_ROOT:-$HOME}"
TARGET="${INSTALL_ROOT}/${HIVE_PKG}"
METASTORE_DIR="${HOME}/.hive_metastore"

# Hadoop (clientes HDFS)
if [ -z "${HADOOP_HOME:-}" ]; then
  for d in /opt/hadoop /usr/local/hadoop "$HOME/hadoop"; do
    if [ -d "$d" ] && [ -x "$d/bin/hdfs" ]; then
      export HADOOP_HOME="$d"
      break
    fi
  done
fi
if [ -z "${HADOOP_HOME:-}" ] || [ ! -d "$HADOOP_HOME" ]; then
  echo "ERROR: Define HADOOP_HOME (directorio con bin/hdfs). Ejemplo: export HADOOP_HOME=/opt/hadoop"
  exit 1
fi
echo "HADOOP_HOME=$HADOOP_HOME"

# Hive 4.2.x está compilado para Java 21 (class file 65). Con Java 17 falla:
# UnsupportedClassVersionError ... only recognizes class file versions up to 61.0
_resolve_java21_home() {
  local j v
  local -a _cand=(
    "${JAVA_HOME:-}"
    /usr/lib/jvm/java-21-openjdk-amd64
    /usr/lib/jvm/java-21-openjdk
    /usr/lib/jvm/java-21-amazon-corretto
  )
  if [ -d "$HOME/.sdkman/candidates/java" ]; then
    shopt -s nullglob
    for j in "$HOME/.sdkman/candidates/java"/21*; do
      _cand+=("$j")
    done
    shopt -u nullglob
  fi
  for j in "${_cand[@]}"; do
    [ -z "${j:-}" ] && continue
    [ -d "$j" ] && [ -x "$j/bin/java" ] || continue
    v="$("$j/bin/java" -version 2>&1 | head -1)"
    if echo "$v" | grep -qE ' version "21\.|openjdk version "21'; then
      echo "$j"
      return 0
    fi
  done
  return 1
}

if ! _jh21="$(_resolve_java21_home)"; then
  echo "ERROR: No se encontró un JDK 21. Hive 4.2 lo requiere (no uses Java 17 para schematool/hive)."
  echo "  Ubuntu/Debian: sudo apt install openjdk-21-jdk"
  echo "  Luego: export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64   # o la ruta que muestre update-java-alternatives -l"
  exit 1
fi
export JAVA_HOME="$_jh21"
echo "JAVA_HOME=$JAVA_HOME (Java 21 — obligatorio para Hive 4.2)"
echo "java: $($JAVA_HOME/bin/java --version 2>&1 | head -1)"

echo ""
echo "=== Descargando Apache Hive ${HIVE_VERSION} ==="
cd /tmp
rm -f "$TGZ"
curl -fSL -o "$TGZ" "${BASE_URL}/${TGZ}"
echo "Extrayendo en ${INSTALL_ROOT}..."
tar -xzf "$TGZ" -C "${INSTALL_ROOT}"

if [ ! -d "$TARGET" ]; then
  echo "ERROR: No se creó $TARGET"
  exit 1
fi

echo ""
echo "=== Apache Derby (driver JDBC embebido / metastore local) ==="
echo "Hive 4.2 a veces no trae derby.jar en lib/; sin él schematool falla con:"
echo "  ClassNotFoundException: org.apache.derby.jdbc.EmbeddedDriver"
DERBY_VER="${DERBY_VERSION:-10.15.2.0}"
DERBY_JAR="derby-${DERBY_VER}.jar"
if ! find "${TARGET}/lib" -maxdepth 1 -name 'derby*.jar' 2>/dev/null | grep -q .; then
  echo "Descargando ${DERBY_JAR} desde Maven Central..."
  curl -fSL -o "${TARGET}/lib/${DERBY_JAR}" \
    "https://repo1.maven.org/maven2/org/apache/derby/derby/${DERBY_VER}/${DERBY_JAR}"
  echo "OK: ${TARGET}/lib/${DERBY_JAR}"
else
  echo "Derby ya presente en ${TARGET}/lib/ (derby*.jar)."
fi

mkdir -p "$METASTORE_DIR"
CONF="${TARGET}/conf/hive-site.xml"

if [ -f "$CONF" ]; then
  echo "Ya existe $CONF — no lo sobrescribo (haz backup si quieres regenerar)."
else
  echo "Creando $CONF (metastore Derby embebido + warehouse HDFS)..."
  # Warehouse relativo al fs.defaultFS de Hadoop (normalmente hdfs://nodo1:9000/user/hive/warehouse)
  cat > "$CONF" << HIVEEOF
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/user/hive/warehouse</value>
    <description>Almacén por defecto en HDFS</description>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:;databaseName=${METASTORE_DIR}/metastore_db;create=true</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.apache.derby.jdbc.EmbeddedDriver</value>
  </property>
  <property>
    <name>hive.metastore.schema.verification</name>
    <value>false</value>
  </property>
  <property>
    <name>hive.execution.engine</name>
    <value>mr</value>
    <description>MapReduce: Hive 4.x puede intentar Tez por defecto; sin Tez instalado falla (TezTaskCommunicatorImpl)</description>
  </property>
</configuration>
HIVEEOF
fi

# hive-env: HADOOP_HOME (Hive carga bin/hadoop desde aquí)
ENV_SH="${TARGET}/conf/hive-env.sh"
if [ -f "${TARGET}/conf/hive-env.sh.template" ] && [ ! -f "$ENV_SH" ]; then
  cp "${TARGET}/conf/hive-env.sh.template" "$ENV_SH"
fi
if [ ! -f "$ENV_SH" ]; then
  echo "# Generado por instalar_hive_java21.sh" > "$ENV_SH"
fi
if ! grep -q "^export HADOOP_HOME=" "$ENV_SH" 2>/dev/null; then
  echo "export HADOOP_HOME=${HADOOP_HOME}" >> "$ENV_SH"
fi
if ! grep -q "^export HIVE_CONF_DIR=" "$ENV_SH" 2>/dev/null; then
  echo "export HIVE_CONF_DIR=${TARGET}/conf" >> "$ENV_SH"
fi
# Hive 4.2 debe usar el mismo JDK 21 (no el Java 17 del sistema/Hadoop)
if grep -q '^export JAVA_HOME=' "$ENV_SH" 2>/dev/null; then
  sed -i.bak "s|^export JAVA_HOME=.*|export JAVA_HOME=${JAVA_HOME}|" "$ENV_SH" && rm -f "${ENV_SH}.bak"
else
  echo "" >> "$ENV_SH"
  echo "# Java 21 requerido por Hive 4.2 (instalar_hive_java21.sh)" >> "$ENV_SH"
  echo "export JAVA_HOME=${JAVA_HOME}" >> "$ENV_SH"
fi
# hadoop jar (schematool) no incluye todo lib/; Derby debe ir en HADOOP_CLASSPATH
if ! grep -q 'smart_energy: Derby → HADOOP_CLASSPATH' "$ENV_SH" 2>/dev/null; then
  cat >> "$ENV_SH" << 'ENVEOF'

# --- smart_energy: Derby → HADOOP_CLASSPATH (schematool / hadoop jar) ---
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
fi
# Java 21: Hive CliDriver (StringInternUtils) requiere --add-opens java.base/java.net
if ! grep -q 'smart_energy: Java 21 --add-opens' "$ENV_SH" 2>/dev/null; then
  cat >> "$ENV_SH" << 'ENVEOF'

# --- smart_energy: Java 21 --add-opens (Hive CliDriver) ---
HIVE_JAVA21_OPENS="--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED"
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} ${HIVE_JAVA21_OPENS}"
# --- fin Java 21 opens ---
ENVEOF
fi

echo ""
echo "=== Esquema metastore (Derby embebido) ==="
if [ -x "${TARGET}/bin/schematool" ] && [ ! -d "${METASTORE_DIR}/metastore_db" ]; then
  # Forzar JAVA_HOME por si el script hive delega en hadoop sin cargar hive-env
  env JAVA_HOME="$JAVA_HOME" PATH="${JAVA_HOME}/bin:${PATH}" "${TARGET}/bin/schematool" -dbType derby -initSchema || {
    echo "AVISO: schematool falló. Prueba manualmente desde $TARGET:"
    echo "  export JAVA_HOME=$JAVA_HOME"
    echo "  export HADOOP_HOME=$HADOOP_HOME"
    echo "  export PATH=\"\$JAVA_HOME/bin:\$PATH\""
    echo "  bin/schematool -dbType derby -initSchema"
  }
fi

echo ""
echo "=== Creando directorios HDFS (warehouse) ==="
"${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p /user/hive/warehouse 2>/dev/null || true
"${HADOOP_HOME}/bin/hdfs" dfs -chmod -R 775 /user/hive 2>/dev/null || true

echo ""
echo "=== Instalación lista ==="
echo "HIVE_HOME=$TARGET"
echo ""
echo "Añade a ~/.bashrc:"
echo "  export JAVA_HOME=\"$JAVA_HOME\""
echo "  export PATH=\"\\\$JAVA_HOME/bin:\\\$PATH\""
echo "  export HIVE_HOME=\"$TARGET\""
echo "  export PATH=\"\\\$HIVE_HOME/bin:\\\$PATH\""
echo "  export HADOOP_HOME=\"$HADOOP_HOME\""
echo ""
echo "Prueba (nueva terminal o source ~/.bashrc):"
echo "  hive -e \"SHOW DATABASES;\""
echo ""
echo "IMPORTANTE: bin/hive y bin/schematool llaman a \$HADOOP_HOME/bin/hadoop, que"
echo "carga ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh y puede FIJAR JAVA_HOME a Java 17."
echo "Si schematool falla con UnsupportedClassVersionError (61 vs 65), edita ese"
echo "hadoop-env.sh y pon: export JAVA_HOME=$JAVA_HOME"
echo "Diagnóstico: ./scripts/diagnostico_java_hadoop_hive.sh"
echo ""
echo "Si schematool falla con ClassNotFoundException EmbeddedDriver (JARs en lib/ pero"
echo "sin classpath): ./scripts/instalar_derby_en_hive.sh  (añade Derby a HADOOP_CLASSPATH)"
echo ""
echo "Nota: ignora avisos SLF4J 'multiple bindings' si el comando termina bien."
rm -f "/tmp/$TGZ"
