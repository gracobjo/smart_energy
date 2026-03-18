"""
Sistema de Monitoreo de Redes de Energía Inteligentes (Smart Grid) - Configuración central
Paths de JARs y parámetros configurables. Stack 2026: Kafka 3.9.1 (KRaft), Spark 3.5.x, Cassandra 5.0, Airflow 2.10.x
"""
import os

BASE_PATH = os.path.expanduser("~/smart_energy")
if not os.path.exists(BASE_PATH):
    BASE_PATH = os.path.dirname(os.path.abspath(__file__))

# JARs - Configurables según instalación
JAR_GRAPHFRAMES = os.environ.get(
    "JAR_GRAPHFRAMES",
    os.path.join(BASE_PATH, "herramientas/graphframes-0.8.3-spark3.5-s_2.12.jar")
)
JAR_CASSANDRA = os.environ.get(
    "JAR_CASSANDRA",
    "/home/hadoop/.ivy2/cache/com.datastax.spark/spark-cassandra-connector_2.12/jars/spark-cassandra-connector_2.12-3.5.0.jar"
)
JAR_KAFKA = os.environ.get(
    "JAR_KAFKA",
    os.path.join(BASE_PATH, "spark-sql-kafka-0-10_2.12-3.5.1.jar")
)

if not os.path.exists(JAR_GRAPHFRAMES):
    alt = os.path.join(BASE_PATH, "graphframes-0.8.3-spark3.5-s_2.12.jar")
    if os.path.exists(alt):
        JAR_GRAPHFRAMES = alt

# APIs externas (clima para correlación demanda; opcional Electricity Maps)
API_WEATHER_KEY = os.environ.get("API_WEATHER_KEY") or os.environ.get("OPENWEATHER_API_KEY") or ""
API_WEATHER_BASE = "https://api.openweathermap.org/data/2.5/weather"
# Electricity Maps: https://api.electricitymaps.com/ (auth-token header)
ELECTRICITY_MAPS_API_KEY = os.environ.get("ELECTRICITY_MAPS_API_KEY", "")
ELECTRICITY_MAPS_ZONE = os.environ.get("ELECTRICITY_MAPS_ZONE", "ES")

# Kafka 3.9.1 (KRaft)
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_RAW = "energy_raw"              # Carga y voltaje por subestación (+ líneas para grafos)
TOPIC_WEATHER_RAW = "weather_raw"     # Clima en zonas solares/eólicas
TOPIC_FILTERED = "energy_filtered"
TOPIC_ENERGY = TOPIC_FILTERED

# Cassandra 5.0 - Estado en tiempo real (último voltaje por subestación)
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "127.0.0.1")
KEYSPACE = "smart_grid"

# HDFS - Backup para batch y re-entrenamiento
HDFS_BACKUP_PATH = "/user/hadoop/energy_backup"

# Hive - Histórico y reportes de consumo energético diario
HIVE_DB = "smart_grid_analytics"
