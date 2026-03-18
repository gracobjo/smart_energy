# Flujo de datos — Smart Grid

## 1. Ingesta (`producer.py`)

Cada ciclo (~15 min) genera:

1. **`energy_raw` (Kafka + HDFS)**  
   - `electricity_maps`: intensidad de carbono, % renovable (API **Electricity Maps** o sintético).  
   - `subestaciones`: voltaje (kV), potencia (MW), uso %, estado.  
   - `lineas`: flujo MW, estado por línea.

2. **`weather_raw` (Kafka)**  
   - `zonas_renovables`: por cada zona en `config_plantas_renovables.py`, datos **OpenWeather** (viento, nubes, temperatura) para correlación con producción solar/eólica.

Detalle de APIs: **[API_INTEGRACION.md](API_INTEGRACION.md)**.

---

## 2. Procesamiento Spark (`procesamiento_grafos.py`)

| Entrada | Salida |
|---------|--------|
| JSON en **HDFS** (`energy_backup`) o simulación | **Cassandra** `smart_grid`: `subestaciones_estado`, `lineas_estado`, `pagerank_subestaciones`, `puntos_fallo_unicos` |
| Grafo filtrado (líneas en sobrecarga fuera) | PageRank + análisis de **articulación** (fragmentación si cae un nodo) |

---

## 3. Streaming (`streaming_ventanas_15min.py`)

Lee **energy_raw**, ventanas **15 min**: carga media, potencia total red, picos.

---

## 4. Hive

| Tabla | Origen |
|-------|--------|
| `sostenibilidad_carbono_hist` | `persistir_hive_ingesta.py` desde JSON energy (carbono + carga media). |
| `clima_renovables_hist` | Mismo script desde JSON weather. |
| Otras (`subestaciones_historico`, consumo diario) | `persistencia_hive.py` |

---

## 5. Checklist técnico (KDD / Lambda-Kappa)

- Fase I: selección y creación del dataset → **producer** + APIs.  
- Fase II–III: transformación, minería → **Spark** (grafos, streaming).  
- Fase IV: interpretación → **dashboard**, reportes Hive.  
- **Cassandra**: último estado para alertas.  
- **Hive**: histórico sostenibilidad y consumo.
