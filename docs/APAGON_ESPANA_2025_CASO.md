# Detección de riesgo de apagón eléctrico — caso de referencia España (2025)

## Contexto: evento real en el sistema eléctrico ibérico (2025)

En **2025** se produjo un **gran apagón** que afectó de forma prolongada a **España y Portugal**, con impacto masivo en suministro, transporte y servicios esenciales. Los informes públicos y el debate sectorial apuntan a una **pérdida de sincronismo** y **inestabilidad de frecuencia** en la interconexión, agravada por **desbalances** entre generación y demanda y por la **propagación en cascada** de desconexiones en la red de transporte y distribución.

Este documento **no reproduce datos operativos confidenciales** de Red Eléctrica (REE) u operadores; define el **marco de detección de riesgo** implementado en el proyecto Smart Grid, **calibrado conceptualmente** a partir de ese tipo de evento:

| Fenómeno observado en apagones en red | Señal en el modelo |
|--------------------------------------|-------------------|
| Tensiones fuera de banda (sobre/sub tensión) | Componente **voltaje** |
| Desviación de 50 Hz / inestabilidad de frecuencia | Componente **frecuencia** (PMU/SCADA simulable) |
| Pérdida de generación o falta de margen ante desconexiones | Componente **generación** (carga vs capacidad agregada) |
| Aislamientos sucesivos, líneas críticas, nodos de corte | Componente **cascada** (líneas anómalas + articulaciones) |

---

## Módulo software: `procesamiento/deteccion_apagon/`

- **`risk_score` (0–100)**: combinación ponderada de cuatro componentes en `[0, 1]`.
- **Alerta crítica**: cuando `risk_score ≥ RIESGO_APAGON_UMBRAL_CRITICO` (por defecto **75**).

### Componentes

1. **Sobretensión / voltaje** — Desviación del voltaje respecto al nominal por subestación; fuera de la banda configurable (±5 % por defecto) aumenta el riesgo.
2. **Inestabilidad de frecuencia** — `|f − 50 Hz|` normalizada respecto a una desviación máxima de referencia (p. ej. 0,5 Hz). Si no hay medida, el término es 0.
3. **Pérdida de margen de generación** — Ratio `potencia_total / capacidad_total` como proxy de falta de margen ante pérdidas de generación.
4. **Desconexión en cascada** — Combinación de la fracción de **líneas en estado no nominal** y la proporción de **puntos de articulación** respecto al tamaño de la red.

### Integración

- **API REST:** `GET /api/v1/riesgo-apagon?frecuencia_hz=` (opcional).
- **Dashboard Streamlit:** bloque **Riesgo de apagón** bajo los KPIs principales (datos en tiempo real desde Cassandra).

### Variables de entorno (operación)

| Variable | Descripción |
|----------|-------------|
| `RIESGO_APAGON_UMBRAL_CRITICO` | Umbral de alerta (0–100) |
| `RIESGO_APAGON_PESO_VOLTAJE` | Peso componente voltaje |
| `RIESGO_APAGON_PESO_FRECUENCIA` | Peso frecuencia |
| `RIESGO_APAGON_PESO_GENERACION` | Peso generación |
| `RIESGO_APAGON_PESO_CASCADA` | Peso cascada |
| `RIESGO_APAGON_FREQ_NOMINAL` | Frecuencia nominal (50 Hz) |
| `RIESGO_APAGON_FREQ_DESV_MAX` | Desviación Hz que satura el componente |
| `SIM_FREQ_HZ` | (Opcional) valor por defecto en UI/API |

---

## Trazabilidad con requisitos del proyecto

- **RF de monitorización y anomalías** — Extensión lógica del pipeline KDD.
- **Documentación de diseño** — Ver `docs/DISENO.md` y `docs/ESPECIFICACION_REQUISITOS.md` (RF-11).

---

## Referencias públicas (lectura)

- Cobertura mediática y análisis del apagón ibérico 2025 (sincronismo, frecuencia, restauración).
- Códigos de red ENTSO-E sobre frecuencia nominal y criterios de operación.

*Documento orientado a defensa académica / presentación del proyecto Smart Grid.*
