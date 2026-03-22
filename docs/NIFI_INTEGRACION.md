# Integración NiFi 2.6.0 — Smart Grid

NiFi complementa o sustituye al script Python para la ingesta: APIs (OpenWeather, Electricity Maps), logs GPS y publicación en Kafka.

---

## Instalación

```bash
cd ~/smart_energy
./scripts/instalar_nifi_260.sh
```

Descarga Apache NiFi 2.6.0 y lo instala en `nifi-2.6.0/` (o `$NIFI_HOME`).

---

## Flujos de ingesta

| Flujo | Procesadores | Descripción |
|-------|--------------|-------------|
| **APIs** | GenerateFlowFile → InvokeHTTP (OpenWeather) → JoltTransformJSON → PublishKafka (weather_raw, energy_raw) | Consume OpenWeather y publica en Kafka |
| **Producer** | GenerateFlowFile → ExecuteStreamCommand (python producer.py) | Ejecuta el script Python completo (APIs + simulación + Kafka + HDFS) |
| **GPS** | GetFile (data/gps_logs) → PublishKafka (gps_raw) | Lee logs GPS del directorio y publica en Kafka |

---

## Arranque desde el dashboard

1. **Fase 0** → Arrancar servicios (HDFS, Kafka, Cassandra, topics).
2. **Fase 1** → expandir **NiFi**:
   - **Arrancar NiFi** (o `$NIFI_HOME/bin/nifi.sh start`)
   - **Crear procesadores NiFi (Fase I demo)** (requiere `NIFI_USER` y `NIFI_PASS` en .env)
   - **Conectar y ejecutar**
   - **Arrancar procesadores**

---

## Directorio de logs GPS

Los ficheros en `data/gps_logs/` se consumen con GetFile y se publican en `gps_raw`. Formato esperado: JSON.

Ejemplo (`data/gps_logs/ejemplo_gps.json`):

```json
{"gps_logs":[{"lat":40.42,"lon":-3.70,"timestamp":"2025-01-01T12:00:00","vehiculo":"mantenimiento_01"}],"fuente":"nifi"}
```

---

## Variables de entorno

| Variable | Descripción |
|----------|-------------|
| `NIFI_HOME` | Directorio de instalación de NiFi |
| `NIFI_USER` | Usuario API NiFi (ver `nifi-app.log`: "Generated Username") |
| `NIFI_PASS` | Contraseña API NiFi (ver `nifi-app.log`: "Generated Password") |
| `NIFI_GPS_LOGS_DIR` | Directorio de logs GPS (por defecto `data/gps_logs`) |
| `API_WEATHER_KEY` | Clave OpenWeather (para InvokeHTTP en NiFi) |
| `KAFKA_BOOTSTRAP` | Servidores Kafka (localhost:9092) |

---

---

## Comprobar flujo, provenance y colas

```bash
# Con credenciales (o NIFI_USER/NIFI_PASS en .env)
python scripts/nifi_flujo_comprobar.py

# Arrancar procesadores STOPPED
python scripts/nifi_flujo_comprobar.py --start
```

Muestra: procesadores, conexiones, colas, provenance, destinos HDFS, Kafka (topics). Ver `docs/INFORME_NIFI_PIPELINE_SMART_GRID.md` para informe detallado.

---

## Flow Definition (grupo completo)

Hay un **flow definition JSON** que puedes importar con todo el flujo integrado:

- Archivo: `nifi/smart_grid_flow_definition.json`
- Incluye: ingesta (producer, OpenWeather, GPS), Kafka (energy_raw, weather_raw, gps_raw), PutHDFS, Spark (procesamiento_grafos → Cassandra + Hive)
- Importación: Crear Process Group → icono upload → seleccionar el JSON
- Ver `nifi/README_FLOW_DEFINITION.md` para detalles

---

## Relación con producer.py

- **NiFi + ExecuteStreamCommand**: NiFi ejecuta `producer.py` cada 15 min (sustituye la ejecución manual).
- **NiFi + InvokeHTTP**: Flujo alternativo solo con OpenWeather (sin Electricity Maps ni simulación completa).
- **NiFi + GetFile**: Ingesta de logs GPS adicionales.
- **Complemento**: Puedes usar NiFi y producer.py a la vez; el procesamiento (Spark) lee de HDFS/Kafka.
