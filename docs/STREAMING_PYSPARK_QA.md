# Pipeline streaming Smart Grid — PySpark, tests y QA

Documentación técnica del módulo `procesamiento/smart_grid_streaming/` y suite **pytest + PySpark**.

---

## PARTE 1 — Especificación de requisitos

### Requisitos funcionales

| ID | Requisito | Descripción |
|----|-----------|-------------|
| RF-ST-01 | **Monitorizar estaciones en tiempo real** | Ingesta de lecturas por subestación (potencia, capacidad, temperatura) con ventanas de agregación y detección de estado. |
| RF-ST-02 | **Detectar anomalías eléctricas** | Regla: `potencia_mw > capacidad_mw` → **sobrecarga**; `temperatura > 80 °C` → **alerta** (correlación térmica / riesgo). |
| RF-ST-03 | **Calcular métricas agregadas** | Media de potencia por ventana (15 min) y subestación, con **watermark** para tolerancia a eventos tardíos. |
| RF-ST-04 | **Consultar histórico** | Persistencia en **Hive** (batch) y métricas/alertas en **Cassandra** para consultas en tiempo real. |

### Requisitos no funcionales

| ID | Requisito | Criterio |
|----|-----------|----------|
| RNF-ST-01 | **Escalabilidad** | Spark Structured Streaming con **YARN** o Kubernetes; particionado por `id_subestacion` en agregaciones. |
| RNF-ST-02 | **Baja latencia** | Estado en **Cassandra** (LOCAL_QUORUM); lecturas sub-ms en operadores. |
| RNF-ST-03 | **Tolerancia a fallos** | Kafka replica + checkpoint Spark; replay de offsets. |
| RNF-ST-04 | **Observabilidad** | Tests automatizados (`pytest`), logging de descartes en limpieza. |

---

## PARTE 2 — Diseño del sistema

### Arquitectura

```
┌─────────┐     ┌──────────────────────────────────────┐     ┌─────────────┐
│  Kafka  │────▶│  Spark Structured Streaming (3.5)  │────▶│  Cassandra  │
│ topics  │     │  limpieza → enriquecimiento →      │     │  estado RT  │
│ energy_*│     │  anomalías → ventanas (watermark)  │     │  alertas    │
└─────────┘     └──────────────────┬─────────────────┘     └─────────────┘
                                   │
                                   ▼
                          ┌────────────────┐
                          │  Hive (batch)  │
                          │  métricas +    │
                          │  histórico     │
                          └────────────────┘
```

**Flujo:** Kafka → Spark → Hive + Cassandra.

### Justificación técnica

| Tecnología | Motivo |
|------------|--------|
| **Spark Streaming** | Procesamiento distribuido y tolerante a fallos; API unificada batch/streaming; ventanas con watermark y state store. |
| **Cassandra** | Escritura/lectura rápida por clave; modelo de estado en tiempo real por subestación; escalado horizontal. |
| **Hive** | Almacén analítico para **histórico** y reportes (particiones por fecha); integración con BI y cuadro de mando. |

### Esquemas (resumen)

**Eventos Kafka (valor JSON simplificado a filas planas en tests):**

- `id_subestacion`, `potencia_mw`, `capacidad_mw`, `temperatura`, `event_time`

**Tabla Hive (ejemplo):** `smart_grid_analytics.metricas_ventanas_15min` — partición `fecha`, columnas `id_subestacion`, `carga_media_mw`, `ventana_inicio`, `ventana_fin`.

**Colección/tabla Cassandra (ejemplo):** `smart_grid.alertas_streaming_batch` — `id_subestacion`, `tipo_alerta`, `potencia_mw`, `capacidad_mw`, `event_ts`.

### Buenas prácticas Spark en código

- `withWatermark("event_time", "10 minutes")` antes de `groupBy(window(...))`.
- `broadcast(maestro)` en JOIN con dimensión pequeña.
- `spark.sql.shuffle.partitions` ajustado en cluster (tests: `local[2]`).

---

## PARTE 3 — Casos de uso

### CU-ST-01 — Monitorización en tiempo real de estaciones

| Campo | Descripción |
|-------|-------------|
| **Actor** | Operador de red |
| **Flujo principal** | 1) Kafka recibe lecturas. 2) Spark limpia y enriquece con maestro. 3) Se calculan ventanas 15 min. 4) Resultados en consola/HDFS/ sink Cassandra. |
| **Resultado esperado** | Vista actualizada de carga media por subestación y ventana; sin datos inválidos. |

### CU-ST-02 — Detección de sobrecargas eléctricas

| Campo | Descripción |
|-------|-------------|
| **Actor** | Operador de red |
| **Flujo principal** | 1) Pipeline aplica `detectar_anomalias`. 2) Si `potencia_mw > capacidad_mw` → `tipo_alerta = sobrecarga`. 3) Upsert a Cassandra / alerta operativa. |
| **Resultado esperado** | Identificación clara de subestaciones en sobrecarga priorizando esta regla sobre alerta térmica. |

### CU-ST-03 — Análisis histórico de consumo energético

| Campo | Descripción |
|-------|-------------|
| **Actor** | Operador de red / analista |
| **Flujo principal** | 1) Spark escribe particiones Hive. 2) Consultas SQL (`spark-sql` / Hive) sobre ventanas y agregados diarios. |
| **Resultado esperado** | Series temporales y comparativas para planificación y auditoría. |

### CU-ST-04 — Gestión de alertas operativas

| Campo | Descripción |
|-------|-------------|
| **Actor** | Operador de red |
| **Flujo principal** | 1) Alertas por sobrecarga o temperatura. 2) Batch preparado para `INSERT` en Cassandra. 3) Dashboard / API consume estado. |
| **Resultado esperado** | Lista de alertas accionables y trazabilidad (`event_ts`). |

---

## Ejecución de tests

```bash
cd ~/smart_energy
source venv/bin/activate
pip install -r requirements.txt
pytest tests/ -v
```

**Cobertura:** limpieza, enriquecimiento (JOIN), anomalías, ventanas, upsert Cassandra (preparación), E2E simulado.

---

## Referencias en el repositorio

- Código: `procesamiento/smart_grid_streaming/`
- Tests: `tests/test_*.py`
- Integración UI: pestaña **Streaming & QA** en el dashboard Streamlit.

---

## Riesgo de apagón eléctrico (complemento)

Módulo **`procesamiento/deteccion_apagon/`**: `risk_score` (0–100) y alertas críticas por sobretensión, frecuencia, margen de generación y cascada. **Caso documentado:** [APAGON_ESPANA_2025_CASO.md](APAGON_ESPANA_2025_CASO.md). API: `GET /api/v1/riesgo-apagon`. Panel en el dashboard bajo los KPIs del mapa.
