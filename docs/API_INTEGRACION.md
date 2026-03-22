# Integración de APIs (Smart Grid)

## API REST propia (Swagger)

El sistema **expone una API REST** documentada con Swagger/OpenAPI para que otros sistemas consuman datos del Smart Grid (subestaciones, líneas, PageRank, puntos de fallo). Ver **[API_SWAGGER.md](API_SWAGGER.md)** — arranque: `./scripts/iniciar_api_smart_grid.sh`, Swagger UI: http://localhost:8000/docs.

---

## APIs públicas consumidas

Este proyecto usa dos familias de datos externos para alimentar el ciclo KDD: **mix eléctrico / carbono** y **meteorología** en zonas renovables.

---

## 1. Electricity Maps API

**Qué aporta:** en tiempo real (por zona/país) el **origen de la electricidad** (solar, eólica, nuclear, gas, carbón, etc.) y la **intensidad de carbono** (g CO₂/kWh). Sirve para:

- Modular la **demanda simulada** por subestación en `producer.py` (más estrés de red cuando baja el % renovable o sube el carbono).
- Alimentar **Hive** (`sostenibilidad_carbono_hist`) para reportes de **sostenibilidad / ESG**.

**Integración en código**

| Elemento | Ubicación |
|----------|-----------|
| Llamadas HTTP | `producer.py` → `obtener_electricity_maps()` |
| Endpoints usados | `GET /v3/carbon-intensity/latest?zone=ES` y `GET /v3/power-breakdown/latest?zone=ES` |
| Autenticación | Cabecera HTTP `auth-token: <API_KEY>` |
| Configuración | `ELECTRICITY_MAPS_API_KEY`, `ELECTRICITY_MAPS_ZONE` (p. ej. `ES`) en `config.py` o variables de entorno |

**Sin clave API:** el productor genera valores **sintéticos** coherentes (`fuente: sintetico_sin_api_key`) para no romper el pipeline en desarrollo.

**Registro y límites:** [electricitymaps.com](https://www.electricitymaps.com/) — suelen ofrecer plan de desarrollo / prueba. Revisa cuotas y términos actuales.

---

## 2. OpenWeather API

**Qué aporta:** condiciones meteorológicas por **coordenadas**. En este proyecto se usa para:

1. **Zonas con plantas solares y eólicas** (`config_plantas_renovables.py`): temperatura, **nubes** (proxy de irradiancia solar), **viento** (producción eólica), humedad. Publicado en Kafka **`weather_raw`** y opcionalmente en Hive **`clima_renovables_hist`**.
2. **Correlación futura** con demanda: temperaturas extremas suelen correlacionar con picos de consumo (climatización); viento/nubes con **producción renovable**.

**Integración en código**

| Elemento | Ubicación |
|----------|-----------|
| Clima por zona renovable | `producer.py` → `clima_zona_renovable()` |
| Endpoint | `https://api.openweathermap.org/data/2.5/weather` (coordenadas `lat`, `lon`) |
| Parámetros | `appid`, `units=metric` |
| Configuración | `API_WEATHER_KEY` en `config.py` o `API_WEATHER_KEY` / `OPENWEATHER_API_KEY` por entorno |

**Registro:** [openweathermap.org](https://openweathermap.org/api) — plan gratuito con límites de llamadas/minuto; suficiente para un ciclo de ingesta cada 15 minutos.

---

## 3. Flujo de datos resumido

```
Electricity Maps  ──► producer  ──► Kafka energy_raw  ──► Spark / HDFS
                         │
OpenWeather (zonas RE) ──┴────────► Kafka weather_raw
                         │
                         └──► (opcional) Hive: sostenibilidad_carbono_hist + clima_renovables_hist
                              PERSIST_HIVE_AFTER_INGEST=1  o  persistir_hive_ingesta.py
```

---

## 4. Variables de entorno recomendadas (producción)

```bash
export ELECTRICITY_MAPS_API_KEY="tu_token"
export ELECTRICITY_MAPS_ZONE="ES"
export API_WEATHER_KEY="tu_openweather_key"
```

No commitear claves en el repositorio.
