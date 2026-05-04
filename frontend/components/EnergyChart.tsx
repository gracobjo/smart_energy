"use client";

import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

export type Point = { t: string; v: number };

export function EnergyChart({ data }: { data: Point[] }) {
  if (!data.length) {
    return <p className="muted">Sin datos todavía. Crea un dispositivo y espera lecturas del simulador.</p>;
  }
  return (
    <div style={{ width: "100%", height: 320 }}>
      <ResponsiveContainer>
        <LineChart data={data} margin={{ left: 8, right: 8, top: 8, bottom: 8 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1f2a44" />
          <XAxis dataKey="t" tick={{ fill: "#94a3b8", fontSize: 11 }} />
          <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} />
          <Tooltip contentStyle={{ background: "#0f172a", border: "1px solid #1f2a44" }} />
          <Line type="monotone" dataKey="v" stroke="#38bdf8" strokeWidth={2} dot={false} name="kWh (sim.)" />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
