// A small set of Carbon-styled primitives, hand-built with Tailwind rather than pulling in the
// @carbon/react library (which is heavy). These carry Carbon's visual language — the g100 dark
// palette, blue-60 interactive color, square-ish 2px radii, IBM Plex type — without its weight.

import { ReactNode, ButtonHTMLAttributes, InputHTMLAttributes, SelectHTMLAttributes } from "react";

export function Button({
  variant = "primary",
  className = "",
  children,
  ...rest
}: ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: "primary" | "secondary" | "danger" | "ghost";
}) {
  const styles: Record<string, string> = {
    primary: "bg-carbon-blue hover:bg-carbon-blue-hover text-white",
    secondary: "bg-carbon-layer2 hover:bg-carbon-border text-carbon-text",
    danger: "bg-carbon-red hover:brightness-110 text-white",
    ghost: "bg-transparent hover:bg-carbon-layer2 text-carbon-text",
  };
  return (
    <button
      className={`px-4 py-2 text-sm font-medium transition-colors disabled:opacity-40 disabled:cursor-not-allowed ${styles[variant]} ${className}`}
      {...rest}
    >
      {children}
    </button>
  );
}

export function TextInput({
  label,
  className = "",
  ...rest
}: InputHTMLAttributes<HTMLInputElement> & { label?: string }) {
  return (
    <label className="block">
      {label && <span className="block text-xs text-carbon-text-3 mb-1">{label}</span>}
      <input
        className={`w-full bg-carbon-field border-b border-carbon-text-3 focus:border-carbon-blue outline-none px-3 py-2 text-sm text-carbon-text placeholder:text-carbon-text-3 ${className}`}
        {...rest}
      />
    </label>
  );
}

export function Select({
  label,
  className = "",
  children,
  ...rest
}: SelectHTMLAttributes<HTMLSelectElement> & { label?: string }) {
  return (
    <label className="block">
      {label && <span className="block text-xs text-carbon-text-3 mb-1">{label}</span>}
      <select
        className={`w-full bg-carbon-field border-b border-carbon-text-3 focus:border-carbon-blue outline-none px-3 py-2 text-sm text-carbon-text ${className}`}
        {...rest}
      >
        {children}
      </select>
    </label>
  );
}

export function Card({
  title,
  actions,
  children,
  className = "",
}: {
  title?: ReactNode;
  actions?: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <div className={`bg-carbon-layer border border-carbon-border ${className}`}>
      {(title || actions) && (
        <div className="flex items-center justify-between px-4 py-3 border-b border-carbon-border">
          <h3 className="text-sm font-semibold text-carbon-text">{title}</h3>
          <div className="flex gap-2">{actions}</div>
        </div>
      )}
      <div className="p-4">{children}</div>
    </div>
  );
}

export function Tag({ children, tone = "gray" }: { children: ReactNode; tone?: "gray" | "blue" | "green" | "red" | "yellow" }) {
  const tones: Record<string, string> = {
    gray: "bg-carbon-layer2 text-carbon-text-2",
    blue: "bg-carbon-blue/20 text-carbon-blue",
    green: "bg-carbon-green/20 text-carbon-green",
    red: "bg-carbon-red/20 text-carbon-red",
    yellow: "bg-carbon-yellow/20 text-carbon-yellow",
  };
  return <span className={`inline-block px-2 py-0.5 text-xs rounded-sm ${tones[tone]}`}>{children}</span>;
}

export function Spinner({ label }: { label?: string }) {
  return (
    <div className="flex items-center gap-2 text-carbon-text-3 text-sm">
      <span className="inline-block w-4 h-4 border-2 border-carbon-text-3 border-t-carbon-blue rounded-full animate-spin" />
      {label}
    </div>
  );
}

export function Banner({ tone, children }: { tone: "error" | "info" | "success"; children: ReactNode }) {
  const tones: Record<string, string> = {
    error: "border-l-carbon-red bg-carbon-red/10 text-carbon-text",
    info: "border-l-carbon-blue bg-carbon-blue/10 text-carbon-text",
    success: "border-l-carbon-green bg-carbon-green/10 text-carbon-text",
  };
  return <div className={`border-l-2 px-4 py-3 text-sm ${tones[tone]}`}>{children}</div>;
}

export function DataTable({
  columns,
  rows,
  empty = "No data",
}: {
  columns: string[];
  rows: ReactNode[][];
  empty?: string;
}) {
  return (
    <div className="overflow-x-auto border border-carbon-border">
      <table className="w-full text-sm border-collapse">
        <thead>
          <tr className="bg-carbon-layer2 text-left">
            {columns.map((c) => (
              <th key={c} className="px-4 py-2 font-semibold text-carbon-text-2 whitespace-nowrap">
                {c}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.length === 0 ? (
            <tr>
              <td colSpan={columns.length} className="px-4 py-6 text-center text-carbon-text-3">
                {empty}
              </td>
            </tr>
          ) : (
            rows.map((r, i) => (
              <tr key={i} className="border-t border-carbon-border hover:bg-carbon-layer2/50">
                {r.map((cell, j) => (
                  <td key={j} className="px-4 py-2 font-mono text-xs text-carbon-text whitespace-nowrap">
                    {cell}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}

export function StatCard({ label, value, tone }: { label: string; value: ReactNode; tone?: "blue" | "green" | "red" }) {
  const color = tone === "green" ? "text-carbon-green" : tone === "red" ? "text-carbon-red" : "text-carbon-text";
  return (
    <div className="bg-carbon-layer border border-carbon-border p-4">
      <div className="text-carbon-text-3 text-xs mb-1">{label}</div>
      <div className={`text-2xl font-mono ${color}`}>{value}</div>
    </div>
  );
}

export function JsonBlock({ value }: { value: unknown }) {
  return (
    <pre className="bg-carbon-field border border-carbon-border p-4 text-xs font-mono text-carbon-text-2 overflow-x-auto">
      {JSON.stringify(value, null, 2)}
    </pre>
  );
}

/// A minimal inline SVG sparkline for the stats view — no charting library.
export function Sparkline({ values, width = 180, height = 40 }: { values: number[]; width?: number; height?: number }) {
  if (values.length < 2) return <span className="text-carbon-text-3 text-xs">collecting…</span>;
  const max = Math.max(...values, 1);
  const min = Math.min(...values, 0);
  const span = max - min || 1;
  const pts = values
    .map((v, i) => {
      const x = (i / (values.length - 1)) * width;
      const y = height - ((v - min) / span) * height;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  return (
    <svg width={width} height={height} className="overflow-visible">
      <polyline points={pts} fill="none" stroke="#0f62fe" strokeWidth="1.5" />
    </svg>
  );
}
