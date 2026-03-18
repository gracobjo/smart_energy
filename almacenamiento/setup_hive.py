from pyspark.sql import SparkSession

# Iniciamos Spark con soporte para Hive
spark = SparkSession.builder \
    .appName("Setup_Hive_Logistica") \
    .enableHiveSupport() \
    .getOrCreate()

# Fase I: Selección y Registro [cite: 18, 21]
spark.sql("CREATE DATABASE IF NOT EXISTS logistica_db")
spark.sql("USE logistica_db")

# Crear tabla de datos maestros para la red de transporte [cite: 4, 24]
spark.sql("""
CREATE TABLE IF NOT EXISTS maestro_vehiculos (
    id_vehiculo STRING,
    conductor STRING,
    ruta_asignada STRING
) USING hive
""")

# Insertar datos de prueba para el cruce de la Fase II 
data = [("bus_01", "Juan Perez", "Ruta_Norte_A1"), 
        ("bus_02", "Maria Lopez", "Ruta_Sur_B2")]
df = spark.createDataFrame(data, ["id_vehiculo", "conductor", "ruta_asignada"])

df.write.mode("overwrite").insertInto("maestro_vehiculos")

print("--- Datos maestros cargados en Hive con éxito ---")
spark.stop()
