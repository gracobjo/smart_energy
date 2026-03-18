"""
Puntos de fallo únicos: si cae una subestación (nodo), ¿en cuántos fragmentos queda la red?
En grafo conexo: nodo de articulación <=> al eliminarlo, quedan ≥2 componentes conexas.
"""
from typing import Dict, List, Set, Tuple, Any


def _construir_adyacencia(nodos: Set[str], aristas: List[Tuple[str, str]]) -> Dict[str, List[str]]:
    ady: Dict[str, List[str]] = {n: [] for n in nodos}
    for u, v in aristas:
        if u in nodos and v in nodos:
            ady[u].append(v)
            ady[v].append(u)
    return ady


def _contar_componentes(nodos: Set[str], ady: Dict[str, List[str]]) -> int:
    visitados: Set[str] = set()
    c = 0
    for n in nodos:
        if n in visitados:
            continue
        c += 1
        pila = [n]
        while pila:
            x = pila.pop()
            if x in visitados:
                continue
            visitados.add(x)
            for y in ady.get(x, []):
                if y in nodos and y not in visitados:
                    pila.append(y)
    return c


def _componentes_como_listas(nodos: Set[str], ady: Dict[str, List[str]]) -> List[Set[str]]:
    visitados: Set[str] = set()
    out: List[Set[str]] = []
    for n in nodos:
        if n in visitados:
            continue
        comp: Set[str] = set()
        pila = [n]
        while pila:
            x = pila.pop()
            if x in visitados:
                continue
            visitados.add(x)
            comp.add(x)
            for y in ady.get(x, []):
                if y in nodos and y not in visitados:
                    pila.append(y)
        out.append(comp)
    return out


def analizar_puntos_fallo_unicos(
    ids_nodos: List[str], aristas_dirigidas: List[Tuple[str, str]]
) -> List[Dict[str, Any]]:
    nodos = set(ids_nodos)
    no_dir: List[Tuple[str, str]] = []
    seen = set()
    for u, v in aristas_dirigidas:
        if u not in nodos or v not in nodos:
            continue
        k = tuple(sorted((u, v)))
        if k not in seen:
            seen.add(k)
            no_dir.append((u, v))

    ady_full = _construir_adyacencia(nodos, no_dir)
    c0 = _contar_componentes(nodos, ady_full)
    grafo_conexo = c0 == 1

    resultados = []
    for v in sorted(nodos):
        resto = nodos - {v}
        aristas_menos_v = [(a, b) for a, b in no_dir if a != v and b != v]
        ady = _construir_adyacencia(resto, aristas_menos_v)
        cv = _contar_componentes(resto, ady)
        if grafo_conexo:
            es_art = cv >= 2
        else:
            es_art = cv > c0

        comps = _componentes_como_listas(resto, ady)
        desc_parts = []
        for i, cset in enumerate(sorted(comps, key=lambda s: -len(s))):
            muestra = sorted(list(cset))[:10]
            desc_parts.append(f"F{i+1}({len(cset)}):{','.join(muestra)}")

        import json
        resultados.append({
            "id_subestacion": v,
            "es_articulacion": es_art,
            "fragmentos_al_fallar": cv,
            "nodos_afectados_json": json.dumps(desc_parts[:8], ensure_ascii=False),
        })
    return resultados
