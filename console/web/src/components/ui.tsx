// A small set of Carbon-styled primitives, hand-built with Tailwind rather than pulling in the
// @carbon/react library (which is heavy). These carry Carbon's visual language — the g100 dark
// palette, blue-60 interactive color, square-ish 2px radii, IBM Plex type — without its weight.

import { ReactNode, ButtonHTMLAttributes, InputHTMLAttributes, SelectHTMLAttributes } from "react";

export function Page({ children, className = "" }: { children: ReactNode; className?: string }) {
  return <div className={`w-full space-y-4 p-3 sm:p-4 lg:p-5 ${className}`}>{children}</div>;
}

export function PageHeader({
  eyebrow,
  title,
  description,
  status,
  actions,
}: {
  eyebrow?: string;
  title: ReactNode;
  description?: ReactNode;
  status?: ReactNode;
  actions?: ReactNode;
}) {
  return <header className="flex flex-wrap items-end justify-between gap-3 border-b border-carbon-border pb-3">
    <div className="min-w-0 max-w-3xl">
      {eyebrow && <div className="mb-2 font-mono text-[10px] uppercase tracking-[0.18em] text-carbon-blue">{eyebrow}</div>}
      <div className="flex flex-wrap items-center gap-3"><h1 className="text-xl font-semibold leading-tight text-carbon-text sm:text-2xl">{title}</h1>{status}</div>
      {description && <p className="mt-1.5 max-w-3xl text-sm leading-5 text-carbon-text-3">{description}</p>}
    </div>
    {actions && <div className="flex shrink-0 flex-wrap items-center gap-2">{actions}</div>}
  </header>;
}

export function EmptyState({ title, description, action }: { title: string; description?: string; action?: ReactNode }) {
  return <div className="border border-dashed border-carbon-border bg-carbon-layer/40 px-5 py-8 text-center">
    <div className="font-medium text-carbon-text">{title}</div>
    {description && <p className="mx-auto mt-2 max-w-md text-sm leading-6 text-carbon-text-3">{description}</p>}
    {action && <div className="mt-5">{action}</div>}
  </div>;
}

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
      className={`min-h-9 px-4 py-2 text-sm font-medium transition-colors focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-carbon-blue disabled:cursor-not-allowed disabled:opacity-40 ${styles[variant]} ${className}`}
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
        className={`w-full border-b border-carbon-text-3 bg-carbon-field px-3 py-2 text-sm text-carbon-text outline-none placeholder:text-carbon-text-3 focus:border-carbon-blue focus-visible:ring-1 focus-visible:ring-carbon-blue ${className}`}
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
        className={`w-full border-b border-carbon-text-3 bg-carbon-field px-3 py-2 text-sm text-carbon-text outline-none focus:border-carbon-blue focus-visible:ring-1 focus-visible:ring-carbon-blue ${className}`}
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
  bodyClassName = "",
}: {
  title?: ReactNode;
  actions?: ReactNode;
  children: ReactNode;
  className?: string;
  /// For a card that has to fill a fixed height and scroll inside itself, rather than growing the
  /// page. The header stays put; only this scrolls.
  bodyClassName?: string;
}) {
  return (
    <div className={`border border-carbon-border bg-carbon-layer ${className}`}>
      {(title || actions) && (
        <div className="flex shrink-0 flex-wrap items-center justify-between gap-2 border-b border-carbon-border px-3 py-2.5">
          <h3 className="text-sm font-semibold text-carbon-text">{title}</h3>
          <div className="flex gap-2">{actions}</div>
        </div>
      )}
      <div className={`p-3 ${bodyClassName}`}>{children}</div>
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
  scrollClassName = "",
}: {
  columns: string[];
  rows: ReactNode[][];
  empty?: string;
  /// Height for the table's own scroll box, e.g. "max-h-[60vh]". Set it when the table can be long:
  /// the header then stays pinned while the rows move, because `sticky` resolves against the
  /// nearest scrolling ancestor and this makes that ancestor the table's own wrapper rather than
  /// the page.
  scrollClassName?: string;
}) {
  return (
    <div className={`overflow-auto border border-carbon-border bg-carbon-layer/30 ${scrollClassName}`}>
      <table className="w-full border-collapse text-sm">
        <thead className="sticky top-0 z-10">
          <tr className="bg-carbon-layer2 text-left">
            {columns.map((c) => (
              <th key={c} className="whitespace-nowrap bg-carbon-layer2 px-3 py-2 font-mono text-[10px] font-normal uppercase tracking-wider text-carbon-text-2">
                {c}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.length === 0 ? (
            <tr>
              <td colSpan={columns.length} className="px-4 py-10 text-center text-carbon-text-3">
                {empty}
              </td>
            </tr>
          ) : (
            rows.map((r, i) => (
              <tr key={i} className="border-t border-carbon-border transition-colors hover:bg-carbon-layer2/50">
                {r.map((cell, j) => (
                  <td key={j} className="whitespace-nowrap px-3 py-2 font-mono text-xs text-carbon-text">
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

export function StatCard({ label, value, tone, detail }: { label: string; value: ReactNode; tone?: "blue" | "green" | "red" | "yellow"; detail?: ReactNode }) {
  const color = tone === "green" ? "text-carbon-green" : tone === "red" ? "text-carbon-red" : tone === "yellow" ? "text-carbon-yellow" : tone === "blue" ? "text-carbon-blue" : "text-carbon-text";
  const rail = tone === "green" ? "border-l-carbon-green" : tone === "red" ? "border-l-carbon-red" : tone === "yellow" ? "border-l-carbon-yellow" : tone === "blue" ? "border-l-carbon-blue" : "border-l-carbon-text-3";
  return (
    <div className={`border border-carbon-border border-l-2 bg-carbon-layer p-3 ${rail}`}>
      <div className="mb-1.5 font-mono text-[10px] uppercase tracking-wider text-carbon-text-3">{label}</div>
      <div className={`font-mono text-xl leading-none ${color}`}>{value}</div>
      {detail && <div className="mt-1.5 text-xs text-carbon-text-3">{detail}</div>}
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
    <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Metric trend" className="h-10 w-full overflow-visible" preserveAspectRatio="none">
      <polyline points={pts} fill="none" stroke="#0f62fe" strokeWidth="1.5" />
    </svg>
  );
}
