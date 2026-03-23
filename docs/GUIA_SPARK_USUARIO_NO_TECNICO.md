# Guía Spark para quien no es informático

Esta guía está integrada en el dashboard (**Fase 2 · Procesamiento**): expander *«Cómo interpretar Spark sin ser informático»* y, tras cada ejecución, un **resumen para decidir**.

---

## Qué mirar (3 preguntas)

1. **¿Ha acabado el trabajo?**  
   Si la pantalla dice que Spark **finalizó correctamente** y el resumen está en **verde**, puede seguir.

2. **¿Los WARN son un problema?**  
   En la práctica, **casi nunca**. Son avisos técnicos (red, Hive, YARN). Lo importante es el **código de salida** y que los datos existan en Cassandra.

3. **¿Qué hago después?**  
   - **Recargar datos** en el mapa.  
   - Opcional: **Comprobar persistencia en Cassandra** para validar antes de un informe a dirección.

---

## Semáforo

| Color en pantalla | Significado para la decisión |
|-------------------|--------------------------------|
| **Verde** | Puede usar el resultado operativo (mapa, informes) salvo comprobación explícita de datos. |
| **Ámbar** | No concluya sin **verificar conteos** o soporte. |
| **Rojo** | **No** use los resultados para decisiones hasta revisión técnica. |

---

## Dónde está el detalle técnico

Solo para **informática o soporte**: expander *«Detalle técnico del log»*.
