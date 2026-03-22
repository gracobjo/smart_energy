#!/bin/bash
# Aplica server.properties KRaft válido para Kafka local (sin 0.0.0.0 en advertised).
# Uso: KAFKA_HOME=~/kafka ./scripts/aplicar_config_kafka_kraft_local.sh
set -e
KAFKA_HOME="${KAFKA_HOME:-$HOME/kafka}"
PROPS="${KAFKA_HOME}/config/kraft/server.properties"
LOG_DIR="${KAFKA_HOME}/logs"

if [ ! -d "$KAFKA_HOME/bin" ]; then
    echo "No existe $KAFKA_HOME/bin. Instala primero: ./scripts/instalar_kafka_local.sh"
    exit 1
fi

"$KAFKA_HOME/bin/kafka-server-stop.sh" 2>/dev/null || true
sleep 1

mkdir -p "$(dirname "$PROPS")" "$LOG_DIR"

cat > "$PROPS" << EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
controller.listener.names=CONTROLLER

listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT

log.dirs=${LOG_DIR}

offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
EOF

echo "Config escrito en: $PROPS"
echo ""
echo "Si es la primera vez o cambiaste log.dirs, formatea storage:"
echo "  rm -rf ${LOG_DIR}/*"
echo "  CLUSTER_ID=\$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)"
echo "  $KAFKA_HOME/bin/kafka-storage.sh format -t \"\$CLUSTER_ID\" -c \"$PROPS\""
echo ""
echo "Arrancar:"
echo "  $KAFKA_HOME/bin/kafka-server-start.sh -daemon $PROPS"
