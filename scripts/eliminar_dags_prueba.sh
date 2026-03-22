#!/usr/bin/bash
# Elimina DAGs de prueba (gemelo, transporte) de la carpeta de DAGs de Airflow.
# Mantiene solo los DAGs de Smart Grid.
# Uso: ./scripts/eliminar_dags_prueba.sh
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DAGS_DIR="${AIRFLOW_HOME:-$HOME/airflow}/dags"

# Si dags_folder está en orquestacion del proyecto, no hay nada que eliminar aquí
if [[ -L "$DAGS_DIR" ]] && [[ "$(readlink -f "$DAGS_DIR")" == *"smart_energy/orquestacion"* ]]; then
  echo "Los DAGs apuntan a orquestacion del proyecto. No hay DAGs de prueba en esa carpeta."
  exit 0
fi

# DAGs de prueba a eliminar (gemelo digital, transporte)
PRUEBA=(
  "dag_maestro_gemelo.py"
  "dag_maestro_transporte.py"
  "dag_setup_gemelo.py"
  "dag_transporte.py"
)

if [[ ! -d "$DAGS_DIR" ]]; then
  echo "No existe $DAGS_DIR (AIRFLOW_HOME=$AIRFLOW_HOME)"
  exit 0
fi

echo "Buscando DAGs de prueba en $DAGS_DIR"
for f in "${PRUEBA[@]}"; do
  path="$DAGS_DIR/$f"
  if [[ -f "$path" ]]; then
    rm -f "$path"
    echo "Eliminado: $f"
  fi
done
echo "Listo. DAGs Smart Grid: dag_arranque_servicios, dag_comprobar_servicios, dag_parar_servicios, dag_maestro, dag_kdd_*, dag_consultas_*"
