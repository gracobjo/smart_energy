#!/bin/bash
# Instala Apache Kafka (KRaft) en /opt/kafka para Smart Grid
# Uso: ./scripts/instalar_kafka_local.sh
set -e

KAFKA_VERSION="${KAFKA_VERSION:-3.9.1}"
SCALA_VERSION="2.13"
INSTALL_DIR="${KAFKA_INSTALL_DIR:-/opt/kafka}"
KAFKA_TGZ="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
TMP_DIR="/tmp/kafka_install_$$"

# Descarga con verificación (curl sin -f puede guardar HTML de error como .tgz)
_download_kafka_tgz() {
    local dest="$1"
    local ver="$2"
    local tgz="kafka_${SCALA_VERSION}-${ver}.tgz"
    local urls=(
        "https://archive.apache.org/dist/kafka/${ver}/${tgz}"
        "https://downloads.apache.org/kafka/${ver}/${tgz}"
    )
    for u in "${urls[@]}"; do
        echo "   Intentando: $u"
        if curl -fSL --connect-timeout 30 --retry 2 -o "$dest" "$u" 2>/dev/null; then
            if gzip -t "$dest" 2>/dev/null; then
                echo "   OK: archivo gzip válido ($(du -h "$dest" | cut -f1))"
                return 0
            fi
            echo "   (respuesta no es gzip, se ignora)"
            rm -f "$dest"
        fi
    done
    return 1
}

echo "=== Instalación de Apache Kafka ${KAFKA_VERSION} (KRaft) ==="
echo "Destino: ${INSTALL_DIR}"
echo ""
if ! command -v java &>/dev/null; then
    echo "Java no encontrado. Instala JDK 11+ antes de continuar."
    exit 1
fi

# Si no hay permisos en /opt, usar ~/kafka
if [ "$INSTALL_DIR" = "/opt/kafka" ] && [ ! -w "/opt" ] 2>/dev/null; then
    INSTALL_DIR="$HOME/kafka"
    echo "Sin permisos en /opt. Instalando en: $INSTALL_DIR"
fi

mkdir -p "$TMP_DIR"
cd "$TMP_DIR"

echo "1. Descargando Kafka ${KAFKA_VERSION}..."
if ! _download_kafka_tgz "$KAFKA_TGZ" "$KAFKA_VERSION"; then
    echo "   Reintentando con 3.9.0..."
    KAFKA_VERSION="3.9.0"
    KAFKA_TGZ="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
    if ! _download_kafka_tgz "$KAFKA_TGZ" "$KAFKA_VERSION"; then
        echo "   Reintentando con 3.8.1..."
        KAFKA_VERSION="3.8.1"
        KAFKA_TGZ="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
        _download_kafka_tgz "$KAFKA_TGZ" "$KAFKA_VERSION" || {
            echo "Error: no se pudo descargar un tarball válido de Kafka."
            echo "Prueba manualmente: curl -fLO https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz"
            exit 1
        }
    fi
fi

echo "2. Extrayendo..."
tar -xzf "$KAFKA_TGZ"
KAFKA_EXTRACTED="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"

echo "3. Instalando en ${INSTALL_DIR}..."
_do() { if [ -w "$(dirname "$INSTALL_DIR")" ] 2>/dev/null; then "$@"; else sudo "$@"; fi; }
_do rm -rf "${INSTALL_DIR}.bak" 2>/dev/null || true
[ -d "$INSTALL_DIR" ] && _do mv "$INSTALL_DIR" "${INSTALL_DIR}.bak"
_do mv "$KAFKA_EXTRACTED" "$INSTALL_DIR"
_do chown -R "$(whoami)" "$INSTALL_DIR" 2>/dev/null || true

cd /
rm -rf "$TMP_DIR"

echo "4. Configurando KRaft (sin Zookeeper)..."
PROPS="${INSTALL_DIR}/config/kraft/server.properties"
if [ -f "$PROPS" ]; then
    # Generar cluster ID y configurar
    CLUSTER_ID=$("${INSTALL_DIR}/bin/kafka-storage.sh" random-uuid 2>/dev/null || echo "MkU3OEVBNTcwNTJENDM2Qk")
    # Crear directorio de logs
    LOG_DIR="${INSTALL_DIR}/logs"
    mkdir -p "$LOG_DIR"
    # Config KRaft completa (faltaba advertised.listeners y listener.security.protocol.map → timeout en topics)
    tee "$PROPS" > /dev/null << EOF
# KRaft - Smart Grid (broker + controller en un nodo)
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
controller.listener.names=CONTROLLER

# Nunca uses 0.0.0.0 en advertised.listeners (Kafka 3.9+ lo rechaza). Local: 127.0.0.1
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT

log.dirs=${LOG_DIR}

# Un solo broker
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
EOF
    # Formatear storage (primera vez; si ya existe metadatos, kafka-storage falla y se ignora)
    "${INSTALL_DIR}/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$PROPS" 2>/dev/null || \
        echo "   (kafka-storage: directorio ya formateado o requiere borrar ${LOG_DIR} para reinstalar)"
else
    echo "   No se encontró config/kraft/server.properties. Usando server.properties clásico."
fi

echo "5. Arrancando Kafka y creando topics energy_raw y weather_raw..."
export PATH="${INSTALL_DIR}/bin:$PATH"
KAFKA_LOG="${INSTALL_DIR}/logs/kafka-install.log"
# Parar instancia previa si quedó colgada
"${INSTALL_DIR}/bin/kafka-server-stop.sh" 2>/dev/null || true
sleep 2

"${INSTALL_DIR}/bin/kafka-server-start.sh" -daemon "${INSTALL_DIR}/config/kraft/server.properties" >"${KAFKA_LOG}" 2>&1 || \
"${INSTALL_DIR}/bin/kafka-server-start.sh" -daemon "${INSTALL_DIR}/config/server.properties" >>"${KAFKA_LOG}" 2>&1 || true

echo "   Esperando broker listo (hasta ~90 s)..."
READY=0
for i in $(seq 1 45); do
    if "${INSTALL_DIR}/bin/kafka-broker-api-versions.sh" --bootstrap-server localhost:9092 >/dev/null 2>&1; then
        # Un segundo más: a veces acepta API pero aún no asigna particiones
        sleep 3
        READY=1
        break
    fi
    sleep 2
done

if [ "$READY" != "1" ]; then
    echo "   AVISO: Kafka no respondió a tiempo. Revisa: ${KAFKA_LOG}"
    tail -30 "${KAFKA_LOG}" 2>/dev/null || true
else
    for topic in energy_raw weather_raw; do
        for attempt in 1 2 3 4 5; do
            if "${INSTALL_DIR}/bin/kafka-topics.sh" --create --topic "$topic" \
                --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1 2>/dev/null; then
                echo "   Topic creado: $topic"
                break
            fi
            sleep 5
        done
    done
fi

echo ""
echo "=== Instalación completada ==="
echo ""
echo "Kafka: ${INSTALL_DIR}"
echo "Broker: localhost:9092"
echo ""
echo "Comandos útiles:"
echo "  Arrancar:  ${INSTALL_DIR}/bin/kafka-server-start.sh -daemon ${INSTALL_DIR}/config/kraft/server.properties"
echo "  Parar:     ${INSTALL_DIR}/bin/kafka-server-stop.sh"
echo "  Topics:    ${INSTALL_DIR}/bin/kafka-topics.sh --list --bootstrap-server localhost:9092"
echo ""
echo "Añade a tu .bashrc si quieres:"
echo "  export KAFKA_HOME=${INSTALL_DIR}"
echo "  export PATH=\$KAFKA_HOME/bin:\$PATH"
echo ""
