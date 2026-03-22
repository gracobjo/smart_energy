# Guía para varios agentes en el mismo código

Cuando más de una persona o agente trabaja en este repositorio, seguir estas reglas reduce conflictos y mantiene `main` estable.

---

## Asignación por componente (Smart Grid)

| Ámbito | Archivos / carpetas | Responsabilidad |
|--------|---------------------|-----------------|
| **Ingesta** | `producer.py`, Kafka/HDFS en `config.py` | API (OpenWeather), simulación carga por subestación, publicación a Kafka `energy_raw` y HDFS. |
| **Procesamiento** | `procesamiento/`, `persistencia_hive.py`, JARs en `config.py` | GraphFrames (subestaciones/líneas), detección nodos críticos, Cassandra, Hive; streaming QA en `procesamiento/smart_grid_streaming/`, `tests/`. |
| **Dashboard y docs** | `app_visualizacion.py`, `app_visualizacion_kdd_panel.py`, `app_streaming_qa_panel.py`, `README.md`, `README_DESPLIEGUE_SMART_GRID.md`, `docs/API_INTEGRACION.md`, `docs/STREAMING_PYSPARK_QA.md`, `AGENTS.md` | Mapa, APIs, despliegue, panel KDD, pestaña Streaming & QA. |
| **Infra y orquestación** | `config_nodos.py`, `config.py`, `orquestacion/`, `cassandra/`, `setup_hive.hql` | Topología red eléctrica, configuración global, DAGs Airflow, esquemas Cassandra/Hive. |

---

## Reglas de ramas

1. **`main`** — Solo código integrado y estable. No hacer commit directo; todo entra por merge/PR.
2. **Rama por tarea** — Usar ramas cortas por feature o fix:
   - `feature/descripcion` — Nueva funcionalidad.
   - `fix/descripcion` — Corrección de bug.
   - `docs/descripcion` — Solo documentación.
3. **Antes de push** — Actualizar tu rama con `main`:
   ```bash
   git fetch origin
   git pull --rebase origin main
   ```
4. **Integrar por Pull Request** — Crear PR de tu rama a `main`; revisar y resolver conflictos en la rama antes de merge.

---

## Resumen

- Un agente por ámbito cuando sea posible.
- Una rama por tarea; integrar a `main` vía PR.
- Siempre `pull --rebase origin main` antes de seguir trabajando o abrir PR.
