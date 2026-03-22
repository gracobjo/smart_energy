# API REST Smart Grid — Swagger/OpenAPI

API REST para exponer datos del Smart Grid a otros sistemas. Documentada con **Swagger (OpenAPI 3)**.

---

## Arranque

```bash
cd ~/smart_energy
source venv/bin/activate   # o venv_transporte
./scripts/iniciar_api_smart_grid.sh
```

O manualmente:

```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000
```

**Variables de entorno:**
- `API_SMART_GRID_PORT` — Puerto (default: 8000)
- `CASSANDRA_HOST` — Host Cassandra (default: 127.0.0.1)

---

## Documentación interactiva

| URL | Descripción |
|-----|-------------|
| http://localhost:8000/docs | **Swagger UI** — Probar endpoints desde el navegador |
| http://localhost:8000/redoc | **ReDoc** — Documentación alternativa |
| http://localhost:8000/openapi.json | Especificación OpenAPI 3 (JSON) |

---

## Endpoints

| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/health` | Health check (API + Cassandra) |
| GET | `/api/v1/subestaciones` | Estado de subestaciones (voltaje, potencia, uso, ubicación) |
| GET | `/api/v1/lineas` | Estado de líneas (flujo MW, capacidad, estado) |
| GET | `/api/v1/pagerank` | PageRank por subestación (nodos críticos) |
| GET | `/api/v1/puntos-fallo` | Puntos de fallo únicos (articulaciones) |
| GET | `/api/v1/red` | Vista consolidada (subestaciones + líneas + PageRank + puntos de fallo) |
| GET | `/api/v1/riesgo-apagon` | **Riesgo de apagón**: `risk_score`, alerta crítica, desglose (opc. `?frecuencia_hz=`) |

---

## Ejemplos de uso

### cURL

```bash
# Health
curl http://localhost:8000/health

# Subestaciones
curl http://localhost:8000/api/v1/subestaciones

# Vista consolidada
curl http://localhost:8000/api/v1/red
```

### Python

```python
import requests

r = requests.get("http://localhost:8000/api/v1/subestaciones")
data = r.json()
for id_sub, info in data["subestaciones"].items():
    print(f"{id_sub}: {info['estado']} — {info['potencia_mw']} MW")
```

### Integración con NiFi

- **InvokeHTTP** — GET a `http://<host>:8000/api/v1/red`
- **PublishKafka** — Publicar el JSON en un topic para downstream

---

## CORS

La API permite CORS desde cualquier origen (`allow_origins=["*"]`) para integración con frontends externos. En producción, restringe los orígenes permitidos.

---

## Requisitos

- Cassandra accesible (keyspace `smart_grid`, tablas: `subestaciones_estado`, `lineas_estado`, `pagerank_subestaciones`, `puntos_fallo_unicos`)
- `cassandra-driver` instalado
- Ver `requirements.txt`: `fastapi`, `uvicorn`
