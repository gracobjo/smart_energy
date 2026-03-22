# -*- coding: utf-8 -*-
"""
Panel de monitorización KDD (importación diferida desde app_visualizacion).
Enlaces en orden, arranques puntuales, explorador Hive y consultas por capa.
"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple


def render_kdd_tools_panel(app: Any, host: str) -> None:
    import streamlit as st

    HIVE_DB = app.HIVE_DB
    KEYSPACE = app.KEYSPACE
    HDFS_BACKUP_PATH = app.HDFS_BACKUP_PATH
    TOPIC_RAW = app.TOPIC_RAW
    TOPIC_WEATHER_RAW = app.TOPIC_WEATHER_RAW
    KAFKA_BOOTSTRAP = app.KAFKA_BOOTSTRAP

    st.markdown("#### Orden sugerido")
    st.info(
        "**1** HDFS (datos) → **2** Kafka → **3** Cassandra → **4** enlaces UI → "
        "**5** Hive / consultas KDD → **6** CQL Cassandra."
    )

    st.markdown("##### 1) Enlaces web (mismo orden que el flujo de datos)")
    ui_host_default = app._get_default_ui_host()
    ui_host = st.text_input(
        "Host para enlaces (IP del servidor si accedes desde fuera)",
        value=ui_host_default,
        key="kdd_panel_ui_host",
        help="YARN a veces solo en 127.0.0.1; configura yarn-site si hace falta.",
    )
    ui_host = (ui_host or ui_host_default).strip() or "localhost"
    st.markdown("**Orquestación y flujos**")
    api_port = getattr(app, "API_SMART_GRID_PORT", 8000)
    c0a, c0b, c0c = st.columns(3)
    with c0a:
        st.link_button("Airflow (8080)", f"http://{ui_host}:8080", use_container_width=True)
        st.caption("Usuario: admin · Contraseña: admin")
    with c0b:
        st.link_button("NiFi (8443)", f"https://{ui_host}:8443/nifi", use_container_width=True)
        st.caption("Credenciales: nifi-app.log")
    with c0c:
        st.link_button("API Swagger", f"http://{ui_host}:{api_port}/docs", use_container_width=True)
        st.caption("REST + OpenAPI · integración externa")
    st.markdown("**Hadoop / Kafka**")
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.link_button("1 · HDFS NN (9870)", f"http://{ui_host}:9870", use_container_width=True)
    with c2:
        st.link_button("2 · YARN (8088)", f"http://{ui_host}:8088", use_container_width=True)
    with c3:
        st.link_button("3 · Job Hist (19888)", f"http://{ui_host}:19888", use_container_width=True)
    with c4:
        st.link_button("4 · Spark Hist (18080)", f"http://{ui_host}:18080", use_container_width=True)
    with c5:
        st.link_button("5 · Kafdrop (9090)", f"http://{ui_host}:9090", use_container_width=True)

    st.markdown("##### 2) Kafka")
    st.caption("Local: `./scripts/instalar_kafka_local.sh` · Docker: `./docker/instalar_docker_kafka.sh`")
    k1, k2 = st.columns(2)
    with k1:
        if st.button("Arrancar Kafka local", type="primary", key="kdd_btn_kafka_local"):
            with st.spinner("Arrancando Kafka..."):
                ok, msg = app._start_kafka()
            (st.success(msg) if ok else st.error(msg))
    with k2:
        if st.button("Kafka + Kafdrop (Docker)", key="kdd_btn_kafka_docker"):
            with st.spinner("Docker..."):
                ok, msg = app._start_kafka_docker()
            (st.success(msg) if ok else st.error(msg))

    st.markdown("##### 3) Cassandra")
    if st.button("Arrancar Cassandra (9042)", key="kdd_btn_cassandra"):
        with st.spinner("Cassandra..."):
            ok, msg = app._start_cassandra()
        (st.success(msg) if ok else st.error(msg))
    if app._port_open("127.0.0.1", 9042):
        st.caption("✓ Puerto 9042 abierto")
    else:
        st.caption("⚠ Puerto 9042 cerrado")

    st.markdown("##### 4) HDFS + YARN")
    h1, h2 = st.columns(2)
    with h1:
        if st.button("HDFS + YARN", key="kdd_btn_hdfs_yarn"):
            with st.spinner("..."):
                ok, msg = app._start_hdfs_yarn()
            (st.success("OK") if ok else st.error("Fallo"))
            st.code(msg[:2000] or "—")
    with h2:
        if st.button("Solo HDFS (start-dfs)", key="kdd_btn_dfs_only"):
            with st.spinner("..."):
                ok, msg = app._start_dfs_only()
            (st.success("OK") if ok else st.error("Fallo"))
            st.code(msg[:2000] or "—")
    y1, y2 = st.columns(2)
    with y1:
        if st.button("Reiniciar YARN", key="kdd_btn_yarn_restart"):
            with st.spinner("..."):
                ok, msg = app._restart_yarn()
            st.caption(msg[:600] if msg else "—")
    with y2:
        if st.button("Job History (19888)", key="kdd_btn_jhs"):
            with st.spinner("..."):
                ok, msg = app._start_job_history_server()
            st.caption(msg[:600] if msg else "—")
    s1, s2 = st.columns(2)
    with s1:
        if st.button("Kafdrop (9090)", key="kdd_btn_kafdrop2"):
            with st.spinner("..."):
                ok, msg = app._start_kafdrop()
            st.caption(msg[:400] if msg else "—")
    with s2:
        if st.button("Spark History (18080)", key="kdd_btn_spark_hist"):
            with st.spinner("..."):
                ok, msg = app._start_spark_history_server()
            st.caption(msg[:400] if msg else "—")

    st.markdown("##### 5) Hive (catálogo histórico)")
    st.caption(
        f"Base **`{HIVE_DB}`** · `setup_hive.hql` · Hive 4.2 + Java 21: `./scripts/instalar_hive_java21.sh`"
    )
    if app._port_open("127.0.0.1", 10000):
        st.caption("✓ HiveServer2 en 10000")
    else:
        st.caption("ℹ HiveServer2 no detectado (normal con solo spark-sql / metastore embebido)")

    _hive_presets: List[Tuple[str, str]] = [
        ("SHOW DATABASES", "SHOW DATABASES;"),
        (f"SHOW TABLES en {HIVE_DB}", f"USE {HIVE_DB}; SHOW TABLES;"),
        (f"Muestra: subestaciones_historico", f"SELECT * FROM {HIVE_DB}.subestaciones_historico LIMIT 20;"),
        (f"Muestra: lineas_historico", f"SELECT * FROM {HIVE_DB}.lineas_historico LIMIT 20;"),
        (f"Muestra: clima_historico", f"SELECT * FROM {HIVE_DB}.clima_historico LIMIT 20;"),
        (f"Muestra: metricas_subestaciones_hist", f"SELECT * FROM {HIVE_DB}.metricas_subestaciones_hist LIMIT 20;"),
        (f"Muestra: eventos_red_historico", f"SELECT * FROM {HIVE_DB}.eventos_red_historico LIMIT 10;"),
        ("SQL personalizado", ""),
    ]

    def _on_hive_preset_change() -> None:
        lab = st.session_state["kdd_hive_expl_preset"]
        sql = next((x[1] for x in _hive_presets if x[0] == lab), "")
        st.session_state["kdd_hive_expl_sql"] = sql or f"USE {HIVE_DB}; SHOW TABLES;"

    def _on_quick_show_tables() -> None:
        # Solo en callback (antes del siguiente rerun): no choca con el text_area(key=...)
        st.session_state["kdd_hive_expl_sql"] = f"USE {HIVE_DB}; SHOW TABLES;"

    if "kdd_hive_expl_sql" not in st.session_state:
        _first_sql = _hive_presets[0][1] or f"USE {HIVE_DB}; SHOW TABLES;"
        st.session_state["kdd_hive_expl_sql"] = _first_sql

    st.selectbox(
        "Plantilla SQL",
        [x[0] for x in _hive_presets],
        key="kdd_hive_expl_preset",
        on_change=_on_hive_preset_change,
    )

    st.button(
        "Listar tablas (rápido)",
        key="kdd_hive_quick_tables",
        on_click=_on_quick_show_tables,
        help="Actualiza el SQL; luego pulsa «Ejecutar» (o vuelve a abrir el expander tras el rerun).",
    )
    _hive_sql_editable = st.text_area(
        "SQL",
        height=100,
        key="kdd_hive_expl_sql",
    )
    _sql_to_run = str(_hive_sql_editable)
    _run_h = st.button("▶ Ejecutar SQL (spark-sql / hive)", type="primary", key="kdd_hive_run")
    if _run_h:
        with st.spinner("Hive / spark-sql..."):
            _h_rc, _h_out = app._ejecutar_consulta_hive(_sql_to_run)
        if _h_rc == 0:
            st.success("rc=0")
        else:
            st.warning(f"rc={_h_rc}")
        _parsed = app._parse_hive_spark_sql_cli_output(_h_out)
        if _parsed:
            st.dataframe(_parsed, use_container_width=True, hide_index=True)
        with st.expander("Salida CLI"):
            st.code(_h_out[:12000] or "(vacío)")

    st.markdown("##### 6) Kafka — muestra sin Docker")
    if app._port_open("127.0.0.1", 9092):
        _ktopic = st.selectbox("Topic", [TOPIC_RAW, TOPIC_WEATHER_RAW], key="kdd_kafka_topic")
        if st.button("Consumir 5 mensajes", key="kdd_kafka_sample"):
            ok, res = app._ejecutar_kafka_consumer_sample(_ktopic, max_messages=5)
            st.json(res) if ok else st.error(str(res))
    else:
        st.info("Kafka no responde en 9092")

    st.markdown("##### 7) YARN — listar jobs")
    if st.button("yarn application -list", key="kdd_yarn_list"):
        rc, out = app._yarn_application_list()
        st.code(out[:3000] or "(vacío)")

    st.markdown("##### 8) Consultas por capa (verificación KDD)")
    q_fase = st.selectbox(
        "Capa",
        [
            "Fase 0: HDFS",
            "Fase 1: HDFS backup + Kafka",
            "Fase 2: Hive (histórico Smart Grid)",
            "Fase 3: Cassandra (tiempo real)",
        ],
        key="kdd_sel_consulta_fase",
    )
    consultas_fase: Dict[str, List[Tuple[str, str, str]]] = {
        "Fase 0: HDFS": [
            ("Listar raíz HDFS", "hdfs dfs -ls /", "hdfs"),
            ("Listar /user/hadoop", "hdfs dfs -ls /user/hadoop", "hdfs"),
        ],
        "Fase 1: HDFS backup + Kafka": [
            (f"Listar backup ({HDFS_BACKUP_PATH})", f"hdfs dfs -ls {HDFS_BACKUP_PATH}", "hdfs"),
            (f"Kafka {TOPIC_RAW}", TOPIC_RAW, "kafka"),
            (f"Kafka {TOPIC_WEATHER_RAW}", TOPIC_WEATHER_RAW, "kafka"),
        ],
        "Fase 2: Hive (histórico Smart Grid)": [
            ("SHOW DATABASES", "SHOW DATABASES;", "hive"),
            (f"USE {HIVE_DB} + SHOW TABLES", f"USE {HIVE_DB}; SHOW TABLES;", "hive"),
            (f"subestaciones_historico", f"SELECT * FROM {HIVE_DB}.subestaciones_historico LIMIT 10;", "hive"),
            (f"lineas_historico", f"SELECT * FROM {HIVE_DB}.lineas_historico LIMIT 10;", "hive"),
            (f"metricas_subestaciones_hist", f"SELECT * FROM {HIVE_DB}.metricas_subestaciones_hist LIMIT 10;", "hive"),
        ],
        "Fase 3: Cassandra (tiempo real)": [
            ("subestaciones_estado", f"SELECT * FROM {KEYSPACE}.subestaciones_estado LIMIT 10", "cql"),
            ("lineas_estado", f"SELECT * FROM {KEYSPACE}.lineas_estado LIMIT 10", "cql"),
            ("pagerank_subestaciones", f"SELECT * FROM {KEYSPACE}.pagerank_subestaciones LIMIT 10", "cql"),
            ("puntos_fallo_unicos", f"SELECT * FROM {KEYSPACE}.puntos_fallo_unicos LIMIT 10", "cql"),
        ],
    }
    for label, cmd, tipo in consultas_fase.get(q_fase, []):
        with st.expander(f"▶ {label}"):
            if tipo == "hdfs":
                st.code(cmd)
                if st.button("Ejecutar", key=f"kdd_run_hdfs_{hash(cmd) % 10**6}"):
                    rc, out = app._ejecutar_consulta_hdfs(cmd)
                    st.code(out[:4000])
                    st.caption(f"rc={rc}")
            elif tipo == "kafka":
                st.caption(f"Topic: {cmd}")
                if st.button("Consumir muestra", key=f"kdd_run_kafka_{hash(cmd) % 10**6}"):
                    ok, res = app._ejecutar_kafka_consumer_sample(cmd)
                    st.json(res) if ok else st.error(str(res))
            elif tipo == "hive":
                st.code(cmd)
                if st.button("Ejecutar", key=f"kdd_run_hive_{hash(cmd) % 10**6}"):
                    rc, out = app._ejecutar_consulta_hive(cmd)
                    st.code(out[:4000])
                    st.caption(f"rc={rc}")
            elif tipo == "cql":
                st.code(cmd)
                if st.button("Ejecutar", key=f"kdd_run_cql_{hash(cmd) % 10**6}"):
                    ok, res = app._ejecutar_consulta_cassandra_cql(cmd, host)
                    if ok:
                        st.dataframe(res, use_container_width=True, hide_index=True) if res else st.info("Sin filas.")
                    else:
                        st.error(str(res))

    st.markdown("##### 9) Explorador CQL (Cassandra)")
    cql_tables = [
        ("subestaciones_estado", f"SELECT * FROM {KEYSPACE}.subestaciones_estado LIMIT 20"),
        ("lineas_estado", f"SELECT * FROM {KEYSPACE}.lineas_estado LIMIT 20"),
        ("pagerank_subestaciones", f"SELECT * FROM {KEYSPACE}.pagerank_subestaciones LIMIT 20"),
        ("puntos_fallo_unicos", f"SELECT * FROM {KEYSPACE}.puntos_fallo_unicos LIMIT 20"),
    ]
    tab_sel = st.selectbox("Tabla", [t[0] for t in cql_tables], key="kdd_sel_cassandra_tabla")
    cql_predef = next((t[1] for t in cql_tables if t[0] == tab_sel), "")
    cql_custom = st.text_area("CQL", value=cql_predef, height=80, key="kdd_cql_editor")
    if st.button("Ejecutar CQL", key="kdd_btn_exec_cql"):
        ok, res = app._ejecutar_consulta_cassandra_cql(cql_custom, host)
        if ok:
            st.dataframe(res, use_container_width=True, hide_index=True) if res else st.info("Sin filas.")
        else:
            st.error(str(res))
