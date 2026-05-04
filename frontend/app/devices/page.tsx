"use client";

import { useEffect, useState } from "react";
import { apiFetch } from "@/lib/api";

type Device = { id: number; name: string; location: string; user_id: number };

export default function DevicesPage() {
  const [items, setItems] = useState<Device[]>([]);
  const [name, setName] = useState("Medidor demo");
  const [location, setLocation] = useState("Madrid");
  const [err, setErr] = useState<string | null>(null);

  async function refresh() {
    setErr(null);
    try {
      const list = await apiFetch<Device[]>("/devices");
      setItems(list);
    } catch (e: unknown) {
      setErr(e instanceof Error ? e.message : "Error");
    }
  }

  useEffect(() => {
    void refresh();
  }, []);

  async function create(e: React.FormEvent) {
    e.preventDefault();
    setErr(null);
    try {
      await apiFetch<Device>("/devices", { method: "POST", body: JSON.stringify({ name, location }) });
      setName("Medidor demo");
      setLocation("Madrid");
      await refresh();
    } catch (e: unknown) {
      setErr(e instanceof Error ? e.message : "Error");
    }
  }

  async function remove(id: number) {
    if (!confirm("¿Eliminar dispositivo?")) return;
    setErr(null);
    try {
      await apiFetch(`/devices/${id}`, { method: "DELETE" });
      await refresh();
    } catch (e: unknown) {
      setErr(e instanceof Error ? e.message : "Error");
    }
  }

  return (
    <div>
      <div className="card">
        <h2 style={{ marginTop: 0 }}>Dispositivos</h2>
        <p className="muted">El backend simula lecturas IoT periódicamente para cada dispositivo.</p>
        {err && <p style={{ color: "#f87171" }}>{err}</p>}
        <form onSubmit={create} className="row" style={{ marginTop: "0.75rem" }}>
          <input value={name} onChange={(e) => setName(e.target.value)} placeholder="Nombre" required />
          <input value={location} onChange={(e) => setLocation(e.target.value)} placeholder="Ubicación" />
          <button type="submit">Añadir</button>
          <button type="button" className="secondary" onClick={() => void refresh()}>
            Refrescar
          </button>
        </form>
      </div>

      <div className="card">
        <h3 style={{ marginTop: 0 }}>Lista</h3>
        {!items.length ? (
          <p className="muted">No hay dispositivos todavía.</p>
        ) : (
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ textAlign: "left", color: "#94a3b8" }}>
                <th>ID</th>
                <th>Nombre</th>
                <th>Ubicación</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {items.map((d) => (
                <tr key={d.id} style={{ borderTop: "1px solid #1f2a44" }}>
                  <td style={{ padding: "0.5rem 0" }}>{d.id}</td>
                  <td>{d.name}</td>
                  <td>{d.location}</td>
                  <td style={{ textAlign: "right" }}>
                    <button type="button" className="secondary" onClick={() => void remove(d.id)}>
                      Eliminar
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
