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

# Kafka 3.9.x (KRaft)
KAFKA_HOME = os.environ.get("KAFKA_HOME", "/opt/kafka")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_RAW = "energy_raw"              # Carga y voltaje por subestación (+ líneas para grafos)
TOPIC_WEATHER_RAW = "weather_raw"     # Clima en zonas solares/eólicas
TOPIC_GPS_RAW = "gps_raw"             # Logs GPS (NiFi GetFile)
TOPIC_FILTERED = "energy_filtered"
TOPIC_ENERGY = TOPIC_FILTERED

# Cassandra 5.0 - Estado en tiempo real (último voltaje por subestación)
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "127.0.0.1")
KEYSPACE = "smart_grid"

# HDFS - Backup para batch y re-entrenamiento
HDFS_BACKUP_PATH = "/user/hadoop/energy_backup"
# Spark History Server: event logs en HDFS (debe coincidir con spark.history.fs.logDirectory)
HDFS_DEFAULT_FS = os.environ.get("HDFS_DEFAULT_FS", "hdfs://nodo1:9000")
SPARK_EVENT_LOG_DIR = os.environ.get("SPARK_EVENT_LOG_DIR", f"{HDFS_DEFAULT_FS}/spark-logs")
SPARK_EVENT_LOG_HDFS_REL = os.environ.get("SPARK_EVENT_LOG_HDFS_REL", "/spark-logs")

# NiFi - Ingesta (API + logs GPS); ver scripts/instalar_nifi_260.sh
NIFI_HOME = os.environ.get("NIFI_HOME", os.path.join(BASE_PATH, "nifi-2.6.0"))
NIFI_GPS_LOGS_DIR = os.environ.get("NIFI_GPS_LOGS_DIR", os.path.join(BASE_PATH, "data", "gps_logs"))

# API Smart Grid - REST con Swagger (puerto 8000, Airflow usa 8080)
API_SMART_GRID_PORT = int(os.environ.get("API_SMART_GRID_PORT", 8000))

# Hive - Histórico y reportes (Hive 4.x + Java 21; ver scripts/instalar_hive_java21.sh)
HIVE_DB = "smart_grid_analytics"
HIVE_HOME = os.environ.get("HIVE_HOME", os.path.expanduser("~/apache-hive-4.2.0-bin"))
SPARK_HOME = os.environ.get("SPARK_HOME", "/opt/spark")
