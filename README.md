# Smart Energy / Smart Grid (España)

Este repositorio contiene **dos líneas de producto** que conviven de forma incremental:

1. **SaaS Smart Energy (MVP)** — API multiusuario, PostgreSQL, panel web Next.js, WebSockets y modelos en `ai/` (scikit-learn).
2. **Smart Grid Big Data (legacy)** — Kafka, Spark, Cassandra, Hive, Airflow y dashboard Streamlit (sin reescritura forzada).

---

## Smart Energy SaaS (MVP)

### Arquitectura

```mermaid
flowchart LR
  subgraph clients [Clientes]
    WEB[Next.js]
  end
  subgraph api [Backend]
    FAST[FastAPI]
    WS[WebSocket /ws/energy]
    SIM[Simulador IoT]
  end
  subgraph data [Datos]
    PG[(PostgreSQL)]
  end
  subgraph ml [IA]
    SK[ai/: RandomForest + IsolationForest]
  end
  WEB -->|REST JWT| FAST
  WEB -->|token query| WS
  FAST --> PG
  SIM --> PG
  SIM --> WS
  FAST --> SK
```

### Estructura de carpetas (nuevo layout)

| Ruta | Rol |
|------|-----|
| `backend/` | FastAPI (`app/`), **Alembic** (`alembic/`, `alembic.ini`), Dockerfile, `requirements.txt`. |
| `frontend/` | Next.js **14.2.35** (parches de seguridad), panel con Recharts y WebSocket. |
| `ai/` | Predicción de consumo y detección de anomalías (importable desde el backend). |
| `infrastructure/` | Documentación de despliegue complementaria; compose en raíz. |
| `docs/` | Especificaciones y diagramas del Smart Grid (existentes). |

### Arranque con Docker

Requisitos: Docker y Docker Compose v2.

```bash
cp .env.example .env   # opcional: ajustar JWT_SECRET y notificaciones
docker compose up --build
```

- **API:** http://localhost:8000/docs  
- **Frontend:** http://localhost:3000  
- **PostgreSQL:** puerto `5432` (usuario/clave `smartenergy` / `smartenergy` en el compose por defecto).

En Docker, el navegador llama a **`/ingest-api/...`** (mismo origen `:3000`); un **Route Handler** de Next reenvía a `BACKEND_INTERNAL_URL` (`http://backend:8000`) para evitar **«Failed to fetch»** y fallos del proxy `rewrites` en modo `standalone`. El WebSocket sigue en `ws://127.0.0.1:8000` en el host.

El **primer usuario registrado** vía `POST /auth/register` recibe rol **admin**. Los siguientes son **user**. Opcionalmente se puede indicar `tenant_name` en el registro para agrupación multi-tenant (tabla `tenants`).

### API REST (resumen)

| Método | Ruta | Descripción |
|--------|------|-------------|
| POST | `/auth/register` | Alta de usuario (Pydantic). |
| POST | `/auth/login` | OAuth2-style JSON → `access_token` JWT. |
| GET | `/users/me` | Perfil actual. |
| GET | `/users` | Lista usuarios (solo **admin**). |
| PATCH | `/users/{id}` | Rol / tenant (solo **admin**). |
| GET/POST/PATCH/DELETE | `/devices` | CRUD dispositivos del usuario (admin ve todos). |
| GET | `/energy/readings?device_id=` | Histórico de lecturas. |
| POST | `/energy/readings?device_id=` | Cuerpo `{"consumption": n}` — lectura manual. |
| GET | `/analytics/predict` | Predicción horaria (histórico en PG). |
| POST | `/analytics/anomalies` | Anomalías por `device_id` o lista `consumptions`. |
| GET | `/analytics/suggestions` | Sugerencias heurísticas de optimización (bonus). |
| GET | `/alerts` | Alertas (filtro opcional `device_id`). |
| GET | `/health` | Salud del servicio. |
| WS | `/ws/energy?token=<JWT>` | Broadcast de lecturas simuladas en JSON. |

**Roles:** cabecera `Authorization: Bearer <token>`.  
**Notificaciones (bonus):** si defines `TELEGRAM_*` o SMTP en `.env`, se intenta notificar al crear alertas por picos.

### Desarrollo local (sin Docker)

```bash
# PostgreSQL en marcha y base smartenergy creada
pip install -r backend/requirements.txt
cd backend
# Windows PowerShell:
$env:DATABASE_URL="postgresql+psycopg2://smartenergy:smartenergy@localhost:5432/smartenergy"
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

En otra terminal:

```bash
cd frontend
npm install
npm run dev
```

El backend inserta el directorio raíz del repo en `sys.path` para poder hacer `import ai` sin configurar `PYTHONPATH` (en Docker se usa `PYTHONPATH=/app`).

### Migraciones (Alembic)

Por defecto (`RUN_ALEMBIC_ON_STARTUP=true` en `.env.example`) el arranque del backend ejecuta **`alembic upgrade head`** antes de levantar el simulador IoT.

Comandos útiles (desde `backend/`, con `DATABASE_URL` configurada):

```bash
alembic current
alembic history
alembic upgrade head
# Nueva revisión a partir de modelos (revisar siempre el diff generado):
alembic revision --autogenerate -m "descripcion_cambio"
```

Si ya tenías tablas creadas solo con `create_all` y al pasar a Alembic aparece error de «relation already exists», marca la revisión inicial sin ejecutarla:

```bash
alembic stamp 0001_initial
```

Para desactivar Alembic en arranque (solo pruebas locales): `RUN_ALEMBIC_ON_STARTUP=false` (el backend usará `create_all`).

### Plan de migración incremental (recomendado)

1. **Fase 0** — Desplegar solo SaaS (`docker compose up`) y validar registro, dispositivos y gráficas.
2. **Fase 1** — Opcional: job que lea agregados del pipeline legacy (p. ej. JSON de `producer.py` o vistas Hive) y escriba en `energy_readings` mediante script ETL (no incluido en el MVP).
3. **Fase 2** — Sustituir o paralelizar el simulador IoT por ingestión real (MQTT/HTTP) publicando al mismo modelo de datos.
4. **Fase 3** — Hardening de secretos, observabilidad (OpenTelemetry, métricas), endurecimiento de despliegue.

### Capturas de pantalla (placeholder)

Sustituir por capturas reales cuando el despliegue esté validado:

- `docs/screenshots/login.png` (pendiente)
- `docs/screenshots/dashboard.png` (pendiente)

### Documentación operativa (SaaS)

- **[docs/SMART_ENERGY_SAAS_OPERACION.md](docs/SMART_ENERGY_SAAS_OPERACION.md)** — Arranque y parada con Docker, registro/login, proxy `/ingest-api`, WebSocket, errores frecuentes y Alembic.

---

## Smart Grid — Monitoreo Big Data (legacy)

Sistema **Big Data** basado en el ciclo **KDD** y arquitectura **Lambda/Kappa**: ingesta con **Python** (Kafka), procesamiento **Spark 3.5** (GraphFrames, streaming 15 min), **Cassandra 5.0** (estado en tiempo real), **Hive** (histórico y sostenibilidad), **Airflow 2.10.x** (orquestación).

---

## Funcionalidades

| Módulo | Descripción |
|--------|-------------|
| **Ingesta** | `producer.py`: **Electricity Maps** (carbono / mix renovable), **OpenWeather** en zonas solares/eólicas; Kafka `energy_raw` + `weather_raw`; backup HDFS. |
| **Procesamiento** | Grafos: subestaciones y líneas; **puntos de fallo únicos** (articulación); PageRank; persistencia Cassandra. |
| **Streaming** | Ventanas **15 min**: carga media de la red, picos de demanda (`streaming_ventanas_15min.py`). |
| **Hive** | Consumo, líneas, **sostenibilidad** (`sostenibilidad_carbono_hist`), **clima renovables** (`clima_renovables_hist`). |
| **Dashboard** | Streamlit: mapa, voltaje/potencia, alertas, PageRank. |
| **Airflow** | DAG maestro (ingesta + batch), mensual (limpieza HDFS, grafos, modelo respaldo). |

**APIs:** ver **[docs/API_INTEGRACION.md](docs/API_INTEGRACION.md)** (Electricity Maps + OpenWeather).

---

## Flujo de datos

```
producer.py → Kafka (energy_raw, weather_raw) + HDFS
       ↓
procesamiento_grafos.py → Cassandra (subestaciones_estado, lineas_estado, puntos_fallo_unicos, …)
       ↓
app_visualizacion.py ← Cassandra
```

Opcional tras ingesta: **`PERSIST_HIVE_AFTER_INGEST=1`** → `persistir_hive_ingesta.py` escribe tablas de sostenibilidad y clima renovable en Hive.

---

## Estructura principal

| Ruta | Rol |
|------|-----|
| `config.py` | Kafka, Cassandra, Hive, claves API |
| `config_nodos.py` | Subestaciones y líneas |
| `config_plantas_renovables.py` | Zonas solares/eólicas (OpenWeather) |
| `producer.py` | Ingesta principal |
| `procesamiento/procesamiento_grafos.py` | Spark + GraphFrames |
| `procesamiento/streaming_ventanas_15min.py` | Structured Streaming |
| `procesamiento/persistir_hive_ingesta.py` | Hive desde JSON energy/weather |
| `persistencia_hive.py` | Histórico subestaciones/consumo |
| `cassandra/esquema_smart_grid.cql` | Esquema Cassandra |
| `setup_hive.hql` | Esquema Hive |
| `orquestacion/` | DAGs Airflow |
| `app_visualizacion.py` | Dashboard Streamlit |

---

## Arranque rápido

```bash
cd /ruta/smart_energy
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Cassandra
cqlsh -f cassandra/esquema_smart_grid.cql

# Kafka (instalación local en /opt/kafka)
./scripts/instalar_kafka_local.sh
# O manual: arrancar kafka-server-start.sh y crear topics energy_raw, weather_raw

# Claves (recomendado por entorno)
export ELECTRICITY_MAPS_API_KEY="..."   # opcional
export API_WEATHER_KEY="..."

python producer.py
python procesamiento/procesamiento_grafos.py
streamlit run app_visualizacion.py
```

**Hive post-ingesta (opcional):**

```bash
export PERSIST_HIVE_AFTER_INGEST=1
python producer.py
# o manual:
python procesamiento/persistir_hive_ingesta.py --energy /tmp/smart_grid_last_energy.json --weather /tmp/smart_grid_last_weather.json
```

---

## Documentación

| Documento | Contenido |
|-----------|-----------|
| [docs/ESPECIFICACION_REQUISITOS.md](docs/ESPECIFICACION_REQUISITOS.md) | Requisitos funcionales y no funcionales |
| [docs/DISENO.md](docs/DISENO.md) | Diseño del sistema, arquitectura, modelo de datos |
| [docs/CASOS_USO.md](docs/CASOS_USO.md) | Casos de uso con escenarios normales y alternativos |
| [docs/DIAGRAMAS_UML.md](docs/DIAGRAMAS_UML.md) | Diagramas UML (Mermaid): casos de uso, secuencia, componentes, etc. |
| [docs/API_INTEGRACION.md](docs/API_INTEGRACION.md) | Electricity Maps y OpenWeather |
| [README_DESPLIEGUE_SMART_GRID.md](README_DESPLIEGUE_SMART_GRID.md) | Servicios, Cassandra, Kafka, troubleshooting |
| [docs/AIRFLOW.md](docs/AIRFLOW.md) | DAGs y carpeta `dags` |
| [docs/FLUJO_DATOS_Y_REQUISITOS.md](docs/FLUJO_DATOS_Y_REQUISITOS.md) | Flujo técnico Smart Grid |
| [docs/SMART_ENERGY_SAAS_OPERACION.md](docs/SMART_ENERGY_SAAS_OPERACION.md) | Operación del MVP SaaS (Docker, proxy, troubleshooting) |
| [AGENTS.md](AGENTS.md) | Convenciones para varios agentes |

---

## Licencia y secretos

No incluir **API keys** en el repositorio; usar variables de entorno o secretos en producción.
