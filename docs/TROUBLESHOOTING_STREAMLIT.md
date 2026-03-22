# Streamlit — errores conocidos

## `StreamlitAPIException` al asignar `st.session_state.hive_expl_sql`

**Causa:** En versiones recientes de Streamlit **no puedes** hacer
`st.session_state.hive_expl_sql = ...` **después** de haber creado
`st.text_area(..., key="hive_expl_sql")` en el mismo script run.

**Síntoma en el traceback:**

```text
File ".../app_visualizacion.py", line XXXX, in main
    st.session_state.hive_expl_sql = _sql_to_run
```

**Qué hacer:**

1. Abre `app_visualizacion.py` en tu máquina y busca `hive_expl_sql`:

   ```bash
   grep -n 'hive_expl_sql' app_visualizacion.py
   ```

2. Si aparece **solo** en el dashboard principal, **elimina** el bloque duplicado del explorador Hive (plantilla + `text_area` + botones) que quedó de una versión antigua. El explorador Hive vive en **`app_visualizacion_kdd_panel.py`** (clave `kdd_hive_expl_sql`, no `hive_expl_sql`).

3. Si necesitas conservar lógica similar:
   - **No** asignes a `st.session_state["hive_expl_sql"]` **después** del `text_area` con esa misma `key`.
   - Opción segura: botón «Listar tablas» **antes** del `text_area`, o `on_change` / `on_click` en callbacks.

4. Opcional: ejecuta el chequeo del repo:

   ```bash
   ./scripts/check_no_legacy_hive_sessionstate.sh
   ```

Tras corregir, reinicia Streamlit (`Ctrl+C` y `streamlit run app_visualizacion.py`).

---

## Hive: metastore Derby incompatible (`ERROR XSLAN`)

**Síntoma:** Al ejecutar `spark-sql` o `hive` aparece:
```text
ERROR XSLAN: Database at /home/hadoop/.hive_metastore/metastore_db has an incompatible format
with the current version of the software. The database was created by or upgraded by version 10.17.
```

**Causa:** El metastore embebido (Derby) se creó con otra versión de Hive/Spark/Java.

**Solución:**
```bash
./scripts/fix_hive_metastore_derby_incompatible.sh
```
Respalda `~/.hive_metastore` y permite que Spark/Hive cree un metastore nuevo. Luego ejecuta de nuevo `setup_hive.hql` o **Fase 0 → Arrancar**.

---

## Hive: `SHOW DATABASES excedió tiempo` / `hive_catalog_ok: false`

**Causa:** La primera consulta tras arrancar Hive/Spark puede tardar mucho (JVM, metastore, Spark SQL engine).

**En la app:** La comprobación del catálogo usa `catalog_probe` (timeouts largos + un reintento). Si aún falla:

```bash
export HIVE_CATALOG_PROBE_HIVE_TIMEOUT_SEC=180
export HIVE_CATALOG_PROBE_SPARK_TIMEOUT_SEC=360
streamlit run app_visualizacion.py
```

Comprueba manualmente cuando el cluster esté estable (la primera ejecución puede tardar 1–2 min; `2>&1` muestra stderr):

```bash
spark-sql -e "SHOW DATABASES;" 2>&1
# o
hive -e "SHOW DATABASES;" 2>&1
```

---

## Hive: `Location: file://... specified for non-external table`

**Síntoma:** Al crear tablas con `spark-sql -f setup_hive.hql` aparecen avisos como:
```text
WARN HiveMetaStore: Location: file:/home/.../spark-warehouse/... specified for non-external table
```

**Causa:** spark-sql usa por defecto un warehouse local (`file://`); Hive recomienda HDFS.

**Solución:** Usar el script que configura el warehouse en HDFS:
```bash
./scripts/aplicar_esquema_hive.sh
```
O bien, manualmente: `spark-sql --conf spark.sql.warehouse.dir=hdfs://localhost:9000/user/hive/warehouse -f setup_hive.hql`

Si las tablas ya existen en `file://`, los avisos son solo informativos; las tablas funcionan. Para tener todo en HDFS, tendrías que hacer DROP de las tablas y volver a ejecutar el script.
