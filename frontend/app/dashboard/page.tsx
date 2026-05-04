"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { EnergyChart, type Point } from "@/components/EnergyChart";
import { API_BASE, WS_BASE, apiFetch, getToken } from "@/lib/api";

type Device = { id: number; name: string; location: string; user_id: number };
type Reading = { id: number; device_id: number; timestamp: string; consumption: number };

export default function DashboardPage() {
  const [devices, setDevices] = useState<Device[]>([]);
  const [deviceId, setDeviceId] = useState<number | null>(null);
  const [points, setPoints] = useState<Point[]>([]);
  const [predict, setPredict] = useState<number[] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const list = await apiFetch<Device[]>("/devices");
        if (cancelled) return;
        setDevices(list);
        if (list.length) setDeviceId((id) => id ?? list[0].id);
      } catch (e: unknown) {
        if (!cancelled) setErr(e instanceof Error ? e.message : "Error cargando dispositivos");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!deviceId) return;
    let cancelled = false;
    (async () => {
      try {
        const readings = await apiFetch<Reading[]>(`/energy/readings?device_id=${deviceId}&limit=120`);
        if (cancelled) return;
        const mapped: Point[] = readings.map((r) => ({
          t: new Date(r.timestamp).toLocaleTimeString(),
          v: r.consumption,
        }));
        setPoints(mapped);
        try {
          const pr = await apiFetch<{ predictions: number[] }>(
            `/analytics/predict?device_id=${deviceId}&horizon_hours=6`,
          );
          if (!cancelled) setPredict(pr.predictions);
        } catch {
          if (!cancelled) setPredict(null);
        }
      } catch (e: unknown) {
        if (!cancelled) setErr(e instanceof Error ? e.message : "Error cargando lecturas");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [deviceId]);

  useEffect(() => {
    const token = getToken();
    if (!token) return;
    const url = `${WS_BASE}/ws/energy?token=${encodeURIComponent(token)}`;
    const ws = new WebSocket(url);
    wsRef.current = ws;
    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data as string);
        if (msg?.type === "energy_reading" && deviceId && msg.device_id === deviceId) {
          setPoints((prev) => {
            const next = [
              ...prev,
              { t: new Date().toLocaleTimeString(), v: Number(msg.consumption) },
            ];
            return next.slice(-180);
          });
        }
      } catch {
        /* ignore */
      }
    };
    return () => {
      ws.close();
      wsRef.current = null;
    };
  }, [deviceId]);

  const chartData = useMemo(() => points, [points]);

  return (
    <div>
      <div className="card">
        <h2 style={{ marginTop: 0 }}>Panel</h2>
        {err && <p style={{ color: "#f87171" }}>{err}</p>}
        <div className="row">
          <label className="muted">
            Dispositivo
            <select
              value={deviceId ?? ""}
              onChange={(e) => setDeviceId(Number(e.target.value))}
              style={{ marginLeft: 8 }}
            >
              {devices.map((d) => (
                <option key={d.id} value={d.id}>
                  {d.name} (#{d.id})
                </option>
              ))}
            </select>
          </label>
          <span className="muted" style={{ fontSize: 13 }}>
            API: {API_BASE}
          </span>
        </div>
      </div>

      <div className="card">
        <h3 style={{ marginTop: 0 }}>Consumo (tiempo real + histórico)</h3>
        <EnergyChart data={chartData} />
      </div>

      <div className="card">
        <h3 style={{ marginTop: 0 }}>Predicción próximas 6 h (RandomForest sobre lags)</h3>
        {!predict?.length ? (
          <p className="muted">Necesitas suficientes lecturas históricas para generar predicción.</p>
        ) : (
          <ul>
            {predict.map((p, i) => (
              <li key={i}>
                +{i + 1}h: <strong>{p.toFixed(2)}</strong> kWh (estimado)
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}
