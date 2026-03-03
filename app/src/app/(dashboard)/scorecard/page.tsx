"use client";

import { useEffect, useState } from "react";
import { Header } from "@/components/layout/header";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { LoadingSpinner } from "@/components/ui/loading";
import { formatIndian, formatPct, cn } from "@/lib/utils";
import {
  TrendingUp,
  TrendingDown,
  Minus,
  ArrowUpDown,
  Zap,
} from "lucide-react";
import type { ScorecardRow } from "@/lib/types";

type SortField = "qtd_volume" | "yoy_pct" | "market_share_pct" | "ev_pct" | "oem_name";
type SortDir = "asc" | "desc";

export default function ScorecardPage() {
  const [data, setData] = useState<ScorecardRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sortField, setSortField] = useState<SortField>("qtd_volume");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  useEffect(() => {
    fetch("/api/scorecard")
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(setData)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  const toggleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDir(sortDir === "asc" ? "desc" : "asc");
    } else {
      setSortField(field);
      setSortDir("desc");
    }
  };

  const sorted = [...data].sort((a, b) => {
    const dir = sortDir === "asc" ? 1 : -1;
    if (sortField === "oem_name") {
      return dir * a.oem_name.localeCompare(b.oem_name);
    }
    return dir * ((a[sortField] ?? 0) - (b[sortField] ?? 0));
  });

  // Compute totals
  const totalVolume = data.reduce((s, r) => s + r.qtd_volume, 0);
  const avgEvPct =
    data.length > 0
      ? data.reduce((s, r) => s + r.ev_pct, 0) / data.length
      : 0;

  if (loading) return <LoadingSpinner className="h-screen" />;
  if (error) {
    return (
      <div className="flex h-screen items-center justify-center">
        <p className="text-lg font-semibold text-red-600">{error}</p>
      </div>
    );
  }

  return (
    <div>
      <Header
        title="OEM Scorecard"
        subtitle="Listed auto OEMs ranked by current-month registration volume"
      />

      <div className="space-y-6 p-6">
        {/* Summary KPIs */}
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
          <Card>
            <CardContent className="pt-5">
              <p className="text-xs font-medium uppercase tracking-wider text-zinc-500">
                Listed OEMs Tracked
              </p>
              <p className="mt-1 text-2xl font-bold">{data.length}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-5">
              <p className="text-xs font-medium uppercase tracking-wider text-zinc-500">
                Total Volume (MTD)
              </p>
              <p className="mt-1 text-2xl font-bold">{formatIndian(totalVolume)}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-5">
              <p className="text-xs font-medium uppercase tracking-wider text-zinc-500">
                Avg EV Penetration
              </p>
              <p className="mt-1 text-2xl font-bold">{avgEvPct.toFixed(1)}%</p>
            </CardContent>
          </Card>
        </div>

        {/* Scorecard Table */}
        <Card>
          <CardHeader>
            <CardTitle>
              OEM Registration Scorecard — Current Month
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-zinc-200 text-left dark:border-zinc-800">
                    <th className="pb-3 pr-4 font-medium text-zinc-500">
                      #
                    </th>
                    <SortableHeader
                      label="OEM"
                      field="oem_name"
                      current={sortField}
                      dir={sortDir}
                      onClick={toggleSort}
                    />
                    <th className="pb-3 px-2 font-medium text-zinc-500 text-center">
                      Ticker
                    </th>
                    <th className="pb-3 px-2 font-medium text-zinc-500 text-center">
                      Segment
                    </th>
                    <SortableHeader
                      label="Volume"
                      field="qtd_volume"
                      current={sortField}
                      dir={sortDir}
                      onClick={toggleSort}
                      align="right"
                    />
                    <SortableHeader
                      label="YoY %"
                      field="yoy_pct"
                      current={sortField}
                      dir={sortDir}
                      onClick={toggleSort}
                      align="right"
                    />
                    <SortableHeader
                      label="Mkt Share"
                      field="market_share_pct"
                      current={sortField}
                      dir={sortDir}
                      onClick={toggleSort}
                      align="right"
                    />
                    <SortableHeader
                      label="EV %"
                      field="ev_pct"
                      current={sortField}
                      dir={sortDir}
                      onClick={toggleSort}
                      align="right"
                    />
                    <th className="pb-3 pl-2 font-medium text-zinc-500 text-right">
                      Est. Rev
                    </th>
                    <th className="pb-3 pl-2 font-medium text-zinc-500 text-center">
                      Conf.
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {sorted.map((row, idx) => (
                    <tr
                      key={row.nse_ticker || row.oem_name}
                      className="border-b border-zinc-100 transition-colors hover:bg-zinc-50 dark:border-zinc-800/50 dark:hover:bg-zinc-900/50"
                    >
                      <td className="py-3 pr-4 text-zinc-400 font-mono text-xs">
                        {idx + 1}
                      </td>
                      <td className="py-3 pr-4 font-medium text-zinc-900 dark:text-zinc-100">
                        {row.oem_name}
                      </td>
                      <td className="py-3 px-2 text-center">
                        <span className="rounded bg-zinc-100 px-1.5 py-0.5 text-xs font-mono text-zinc-600 dark:bg-zinc-800 dark:text-zinc-400">
                          {row.nse_ticker || "—"}
                        </span>
                      </td>
                      <td className="py-3 px-2 text-center">
                        <SegmentBadge segment={row.segment} />
                      </td>
                      <td className="py-3 px-2 text-right font-mono text-zinc-700 dark:text-zinc-300">
                        {formatIndian(row.qtd_volume)}
                      </td>
                      <td className="py-3 px-2 text-right">
                        <YoYCell value={row.yoy_pct} />
                      </td>
                      <td className="py-3 px-2 text-right font-mono text-zinc-700 dark:text-zinc-300">
                        {row.market_share_pct > 0
                          ? `${row.market_share_pct.toFixed(1)}%`
                          : "—"}
                      </td>
                      <td className="py-3 px-2 text-right">
                        {row.ev_pct > 0 ? (
                          <span className="inline-flex items-center gap-0.5 text-emerald-600 font-mono">
                            <Zap className="h-3 w-3" />
                            {row.ev_pct.toFixed(1)}%
                          </span>
                        ) : (
                          <span className="text-zinc-400">—</span>
                        )}
                      </td>
                      <td className="py-3 pl-2 text-right font-mono text-zinc-700 dark:text-zinc-300">
                        {row.est_rev_cr !== null
                          ? `${formatIndian(row.est_rev_cr)} Cr`
                          : "—"}
                      </td>
                      <td className="py-3 pl-2 text-center">
                        <ConfidenceBadge level={row.confidence} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {data.length === 0 && (
              <div className="py-12 text-center text-zinc-500">
                No scorecard data available. Run the ETL pipeline to populate data.
              </div>
            )}
          </CardContent>
        </Card>

        <p className="text-center text-xs text-zinc-400">
          Source: VAHAN (MoRTH). Registrations are not sales. Revenue
          estimates are demand-based proxies using registrations x assumed ASPs.
          This is NOT accounting revenue.
        </p>
      </div>
    </div>
  );
}

// ── Sub-components ──

function SortableHeader({
  label,
  field,
  current,
  dir,
  onClick,
  align = "left",
}: {
  label: string;
  field: SortField;
  current: SortField;
  dir: SortDir;
  onClick: (f: SortField) => void;
  align?: "left" | "right";
}) {
  const isActive = current === field;
  return (
    <th
      className={cn(
        "pb-3 px-2 font-medium text-zinc-500 cursor-pointer select-none hover:text-zinc-900 dark:hover:text-zinc-100 transition-colors",
        align === "right" && "text-right"
      )}
      onClick={() => onClick(field)}
    >
      <span className="inline-flex items-center gap-1">
        {label}
        <ArrowUpDown
          className={cn(
            "h-3 w-3",
            isActive ? "text-blue-600" : "text-zinc-300"
          )}
        />
      </span>
    </th>
  );
}

function SegmentBadge({ segment }: { segment: string }) {
  const colors: Record<string, string> = {
    PV: "bg-blue-100 text-blue-700 dark:bg-blue-950 dark:text-blue-400",
    CV: "bg-amber-100 text-amber-700 dark:bg-amber-950 dark:text-amber-400",
    "2W": "bg-emerald-100 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-400",
  };
  return (
    <span
      className={cn(
        "rounded px-1.5 py-0.5 text-[10px] font-bold",
        colors[segment] || "bg-zinc-100 text-zinc-600"
      )}
    >
      {segment}
    </span>
  );
}

function YoYCell({ value }: { value: number }) {
  if (value === 0) {
    return (
      <span className="inline-flex items-center gap-0.5 text-zinc-400 font-mono">
        <Minus className="h-3 w-3" />
        0.0%
      </span>
    );
  }
  const positive = value > 0;
  return (
    <span
      className={cn(
        "inline-flex items-center gap-0.5 font-mono",
        positive ? "text-emerald-600" : "text-red-600"
      )}
    >
      {positive ? (
        <TrendingUp className="h-3 w-3" />
      ) : (
        <TrendingDown className="h-3 w-3" />
      )}
      {formatPct(value)}
    </span>
  );
}

function ConfidenceBadge({ level }: { level: string }) {
  const styles: Record<string, string> = {
    HIGH: "bg-emerald-100 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-400",
    MEDIUM: "bg-amber-100 text-amber-700 dark:bg-amber-950 dark:text-amber-400",
    LOW: "bg-red-100 text-red-700 dark:bg-red-950 dark:text-red-400",
  };
  return (
    <span
      className={cn(
        "rounded px-1.5 py-0.5 text-[10px] font-bold uppercase",
        styles[level] || styles.MEDIUM
      )}
    >
      {level}
    </span>
  );
}
