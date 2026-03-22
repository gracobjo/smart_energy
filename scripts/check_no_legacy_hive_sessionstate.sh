#!/usr/bin/bash
# Falla si app_visualizacion.py contiene la clave legacy hive_expl_sql (provoca StreamlitAPIException).
set -euo pipefail
BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
F="$BASE/app_visualizacion.py"
if [[ ! -f "$F" ]]; then
  echo "OK (sin $F)"
  exit 0
fi
if grep -q 'hive_expl_sql' "$F"; then
  echo "ERROR: $F contiene 'hive_expl_sql' (código legacy)." >&2
  echo "  Elimina el explorador Hive duplicado o las asignaciones a session_state tras text_area." >&2
  echo "  Ver: docs/TROUBLESHOOTING_STREAMLIT.md" >&2
  exit 1
fi
echo "OK: no hay hive_expl_sql en app_visualizacion.py"
