# Guía rápida: qué decidir cuando ves mensajes de Spark o Hadoop

**Para:** responsables de operación, analistas o dirección — **no hace falta ser informático.**

---

## En 30 segundos: 3 preguntas

1. **¿Aparece la palabra `ERROR` o `Exception` y el proceso se para?**  
   → Algo ha fallado de verdad. Anota la hora y pide revisión a quien administra el cluster.

2. **¿Solo ves `WARN` (avisos) y al final pone un tiempo (`Time taken`) o “finished”?**  
   → Lo normal en muchos entornos. **Puedes considerar el trabajo como completado** si el resultado (datos, informe) es el esperado.

3. **¿Hay una URL tipo `Spark Web UI` o un `Application Id`?**  
   → Suele ser **buena señal**: el sistema ha registrado el trabajo y puedes pedir el enlace si necesitas detalle operativo.

---

## Cómo leer el “semáforo” mental

| Lo que ves en pantalla | Qué implica para **tu decisión** |
|------------------------|----------------------------------|
| Muchas líneas con **WARN** | Avisos habituales. **No significan solos que algo esté mal.** Mira si el proceso **termina** y el entregable es correcto. |
| **ERROR**, **FAILED**, **Exception** | Hay un **problema real**. No asumas que los datos son fiables hasta que lo revise sistemas. |
| **Time taken: X seconds** | El trabajo **ha medido su duración** — útil para planificar ventanas de tiempo en informes. |
| Mensaje de **hostname / IP / 127.0.0.1** | Ajuste interno de red en servidores o máquinas virtuales. **Casi nunca requiere decisión de negocio.** |
| **Native library… using builtin-java** | El sistema usa un modo compatible. **Suele ser aceptable** salvo que tengáis acuerdos de rendimiento muy estrictos. |
| **Neither spark.yarn.jars… falling back to uploading** | El arranque puede ser **más lento** la primera vez. **No implica fallo**; si los plazos son críticos, es tema de optimización técnica. |
| **HiveConf… does not exist** | Aviso de configuración entre versiones. **Si el resultado final es correcto**, no suele afectar a la decisión del día a día. |
| **global_temp / NoSuchObjectException** (solo aviso) | A veces aparece al usar Hive/Spark. **Si todo lo demás está bien**, suele ser secundario. |

---

## Qué hacer según tu rol

| Si eres… | Decisión práctica |
|----------|-------------------|
| **Responsable de un informe o KPI** | Confía en el **resultado final** (tablas, ficheros, dashboard). Los WARN no sustituyen a comprobar que los números cuadran con la fuente de verdad. |
| **Gestor de plazos** | Usa el **tiempo total** (`Time taken`) como referencia de duración. Si es muy largo de forma recurrente, escalad **rendimiento**, no “error” por los WARN. |
| **Quien debe auditar** | Pedid **captura o ID de aplicación** (`Application Id`) y hora exacta para trazabilidad. |

---

## Cuándo **sí** debes escalar a sistemas / informática

- Aparece **ERROR** o el proceso **no termina**.
- Los **datos finales** no coinciden con lo esperado (ceros, vacíos, fechas raras).
- **Antes** de un cierre contable o regulatorio, si es la **primera vez** que ejecutáis algo crítico en ese entorno.

---

## Frase para copiar en un correo (si hace falta ayuda)

> Hemos ejecutado un proceso Spark en YARN. [Adjunto captura.]  
> ¿Terminó correctamente? Vemos muchos WARN.  
> Application Id: …  
> Resultado esperado: …  
> ¿Necesitamos actuar?

---

*Esta guía no sustituye a la documentación técnica del cluster; sirve para **decidir** sin interpretar cada línea de log.*
