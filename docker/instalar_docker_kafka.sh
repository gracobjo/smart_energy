#!/bin/bash
# Instala Docker y arranca Kafka + Kafdrop para Smart Grid
set -e
cd "$(dirname "$0")/.."

echo "=== 1. Instalando Docker y Docker Compose ==="
if command -v docker &>/dev/null; then
    echo "Docker ya instalado: $(docker --version)"
else
    sudo apt-get update
    sudo apt-get install -y docker.io
    sudo systemctl enable docker
    sudo systemctl start docker
    sudo usermod -aG docker "$USER" 2>/dev/null || true
    echo "Docker instalado. Si fallan permisos, ejecuta: newgrp docker"
fi
# Docker Compose: plugin v2 o binario standalone
COMPOSE_CMD=""
if docker compose version &>/dev/null; then
    COMPOSE_CMD="docker compose"
else
    echo "Instalando Docker Compose..."
    if sudo apt-get install -y docker-compose-plugin 2>/dev/null; then
        COMPOSE_CMD="docker compose"
    else
        # Repo oficial Docker (Linux Mint / Ubuntu sin el paquete)
        echo "Añadiendo repositorio oficial de Docker..."
        sudo apt-get update
        sudo apt-get install -y ca-certificates curl
        sudo install -m 0755 -d /etc/apt/keyrings
        curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg 2>/dev/null || true
        sudo chmod a+r /etc/apt/keyrings/docker.gpg 2>/dev/null || true
        CODENAME="$([ -f /etc/os-release ] && . /etc/os-release && echo "${UBUNTU_CODENAME:-${VERSION_CODENAME:-noble}}")"
        echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu ${CODENAME} stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
        sudo apt-get update
        if sudo apt-get install -y docker-compose-plugin 2>/dev/null; then
            COMPOSE_CMD="docker compose"
        fi
    fi
    if [ -z "$COMPOSE_CMD" ]; then
        # Fallback: descargar binario standalone desde GitHub
        echo "Descargando docker-compose desde GitHub..."
        COMPOSE_VER="v2.24.5"
        sudo curl -sSL "https://github.com/docker/compose/releases/download/${COMPOSE_VER}/docker-compose-linux-x86_64" -o /usr/local/bin/docker-compose
        sudo chmod +x /usr/local/bin/docker-compose
        COMPOSE_CMD="docker-compose"
    fi
fi

# Usar 'docker' o 'sudo docker' según permisos
DOCKER="docker"
docker info &>/dev/null || DOCKER="sudo docker"

# Liberar puertos 9092/9093 si están en uso (Kafka nativo, etc.)
for port in 9092 9093; do
  if command -v ss &>/dev/null && ss -tlnp 2>/dev/null | grep -q ":$port "; then
    echo "Puerto $port en uso. Liberando..."
    sudo fuser -k $port/tcp 2>/dev/null || true
    sleep 2
  fi
done

echo ""
echo "=== 2. Arrancando Kafka + Kafdrop ==="
# Limpiar contenedores previos fallidos
if [ "$COMPOSE_CMD" = "docker compose" ]; then
    $DOCKER compose -f docker/docker-compose-kafka.yml down 2>/dev/null || true
    $DOCKER compose -f docker/docker-compose-kafka.yml up -d
else
    docker-compose -f docker/docker-compose-kafka.yml down 2>/dev/null || sudo docker-compose -f docker/docker-compose-kafka.yml down 2>/dev/null || true
    docker-compose -f docker/docker-compose-kafka.yml up -d 2>/dev/null || sudo docker-compose -f docker/docker-compose-kafka.yml up -d
fi

# Si Kafka falló, mostrar logs para diagnosticar
sleep 3
if ! $DOCKER ps --format '{{.Names}}' 2>/dev/null | grep -q smartgrid-kafka; then
    echo ""
    echo "--- Logs de Kafka (falló al arrancar) ---"
    $DOCKER logs smartgrid-kafka 2>&1 | tail -50
    echo "---"
    exit 1
fi

echo ""
echo "Esperando que Kafka esté listo..."
sleep 15

echo ""
echo "=== 3. Creando topics energy_raw y weather_raw ==="
$DOCKER exec smartgrid-kafka /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic energy_raw --bootstrap-server kafka:9092 --partitions 2 --replication-factor 1 2>/dev/null || true
$DOCKER exec smartgrid-kafka /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic weather_raw --bootstrap-server kafka:9092 --partitions 2 --replication-factor 1 2>/dev/null || true

echo ""
echo "=== Listo ==="
echo "Kafka: localhost:9092"
echo "Kafdrop UI: http://localhost:9090"
