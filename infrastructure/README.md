# Infraestructura SaaS (MVP)

El arranque principal del stack SaaS está en la raíz del repositorio: `docker-compose.yml` (backend FastAPI + PostgreSQL + frontend Next.js).

La carpeta `infrastructure/` agrupa artefactos que pueden crecer con el tiempo (por ejemplo manifiestos Kubernetes, Terraform o proxies) sin mezclarlos con el código de aplicación.

El pipeline **Smart Grid** legacy (Kafka, Spark, Cassandra, Airflow) sigue documentado en la raíz y en `orquestacion/`; no forma parte del `docker-compose` del SaaS MVP.
