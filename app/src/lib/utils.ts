import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

/** Format large numbers: 1234567 → "12.3L" (Indian lakh) */
export function formatIndian(n: number): string {
  if (n >= 10_000_000) return `${(n / 10_000_000).toFixed(1)}Cr`;
  if (n >= 100_000) return `${(n / 100_000).toFixed(1)}L`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString("en-IN");
}

/** Format percentage: 12.345 → "+12.3%" */
export function formatPct(n: number): string {
  const sign = n >= 0 ? "+" : "";
  return `${sign}${n.toFixed(1)}%`;
}

/** Format date: "2026-03-01" → "01 Mar 2026" */
export function formatDate(dateStr: string): string {
  const d = new Date(dateStr + "T00:00:00");
  return d.toLocaleDateString("en-IN", {
    day: "2-digit",
    month: "short",
    year: "numeric",
  });
}

/** Segment code to display label */
export function segmentLabel(code: string): string {
  const map: Record<string, string> = {
    PV: "Passenger Vehicles",
    CV: "Commercial Vehicles",
    "2W": "Two-Wheelers",
  };
  return map[code] || code;
}

/** Segment colors for charts */
export const SEGMENT_COLORS: Record<string, string> = {
  PV: "#3b82f6", // blue-500
  CV: "#f59e0b", // amber-500
  "2W": "#10b981", // emerald-500
};

/** EV vs ICE colors */
export const POWERTRAIN_COLORS = {
  EV: "#22c55e", // green-500
  ICE: "#64748b", // slate-500
};
