"use client";

import { useEffect, useState } from "react";
import { apiFetch } from "@/lib/api";

type Alert = {
  id: number;
  device_id: number;
  type: string;
  message: string;
  created_at: string;
};

export default function AlertsPage() {
  const [items, setItems] = useState<Alert[]>([]);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const list = await apiFetch<Alert[]>("/alerts?limit=100");
        if (!cancelled) setItems(list);
      } catch (e: unknown) {
        if (!cancelled) setErr(e instanceof Error ? e.message : "Error");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <div className="card">
      <h2 style={{ marginTop: 0 }}>Alertas</h2>
      <p className="muted">Generadas automáticamente ante picos de consumo (simulador + reglas).</p>
      {err && <p style={{ color: "#f87171" }}>{err}</p>}
      {!items.length ? (
        <p className="muted">Sin alertas todavía.</p>
      ) : (
        <ul style={{ paddingLeft: "1.1rem" }}>
          {items.map((a) => (
            <li key={a.id} style={{ marginBottom: "0.6rem" }}>
              <strong>{a.type}</strong> — dispositivo {a.device_id}{" "}
              <span className="muted">({new Date(a.created_at).toLocaleString()})</span>
              <div>{a.message}</div>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
