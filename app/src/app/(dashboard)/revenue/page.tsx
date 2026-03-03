"use client";

import { useEffect, useState } from "react";
import { Header } from "@/components/layout/header";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { KPICard } from "@/components/ui/kpi-card";
import { LoadingSpinner } from "@/components/ui/loading";
import { formatIndian, cn } from "@/lib/utils";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ErrorBar,
} from "recharts";
import { AlertTriangle, Info } from "lucide-react";
import type { RevenueEstimate } from "@/lib/types";

export default function RevenuePage() {
  const [data, setData] = useState<RevenueEstimate[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch("/api/revenue")
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(setData)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <LoadingSpinner className="h-screen" />;
  if (error) {
    return (
      <div className="flex h-screen items-center justify-center">
        <p className="text-lg font-semibold text-red-600">{error}</p>
      </div>
    );
  }

  // Top-level aggregates
  const totalRevCr = data.reduce((s, r) => s + r.est_domestic_rev_cr, 0);
  const totalVolume = data.reduce((s, r) => s + r.reg_volume, 0);
  const avgCompleteness =
    data.length > 0
      ? data.reduce((s, r) => s + r.data_completeness_pct, 0) / data.length
      : 0;
  const fyQuarter = data.length > 0 ? data[0].fy_quarter : "—";

  // Chart data (top 10 OEMs by revenue)
  const chartData = data.slice(0, 10).map((row) => ({
    name: row.nse_ticker || row.oem_name.slice(0, 12),
    revenue: row.est_domestic_rev_cr,
    low: row.est_domestic_rev_cr - row.est_rev_low_cr,
    high: row.est_rev_high_cr - row.est_domestic_rev_cr,
  }));

  return (
    <div>
      <Header
        title="Revenue Proxy Estimator"
        subtitle={`Demand-based domestic revenue estimates for ${fyQuarter}`}
      />

      <div className="space-y-6 p-6">
        {/* Disclaimer Banner */}
        <div className="flex items-start gap-3 rounded-xl border border-amber-200 bg-amber-50 p-4 dark:border-amber-800 dark:bg-amber-950/30">
          <AlertTriangle className="mt-0.5 h-5 w-5 shrink-0 text-amber-600" />
          <div className="text-sm text-amber-800 dark:text-amber-300">
            <p className="font-semibold">Important Disclaimer</p>
            <p className="mt-1">
              Demand-based proxy using registrations x assumed ASPs. This is{" "}
              <strong>NOT accounting revenue</strong>. Actual company revenues
              differ due to exports, spare parts, financial services, channel
              inventory, and corporate sales mix. Use for directional
              demand-tracking only.
            </p>
          </div>
        </div>

        {/* KPI Row */}
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
          <KPICard
            title={`Total Est. Rev (${fyQuarter})`}
            value={totalRevCr}
            subtitle="Cr (demand proxy)"
          />
          <KPICard
            title="Total Registrations"
            value={totalVolume}
            subtitle="All listed OEMs"
          />
          <div className="rounded-xl border border-zinc-200 bg-white p-5 shadow-sm dark:border-zinc-800 dark:bg-zinc-950">
            <p className="text-xs font-medium uppercase tracking-wider text-zinc-500">
              OEMs Covered
            </p>
            <p className="mt-2 text-2xl font-bold">{data.length}</p>
            <p className="mt-1 text-xs text-zinc-400">Listed entities</p>
          </div>
          <div className="rounded-xl border border-zinc-200 bg-white p-5 shadow-sm dark:border-zinc-800 dark:bg-zinc-950">
            <p className="text-xs font-medium uppercase tracking-wider text-zinc-500">
              Data Completeness
            </p>
            <p className="mt-2 text-2xl font-bold">
              {avgCompleteness.toFixed(0)}%
            </p>
            <p className="mt-1 text-xs text-zinc-400">
              Avg quarter coverage
            </p>
          </div>
        </div>

        {/* Revenue Chart — Top 10 */}
        <Card>
          <CardHeader>
            <CardTitle>
              Top 10 OEMs — Estimated Domestic Revenue ({fyQuarter})
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-80">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart
                  data={chartData}
                  layout="vertical"
                  margin={{ left: 10, right: 30 }}
                >
                  <CartesianGrid strokeDasharray="3 3" stroke="#e4e4e7" />
                  <XAxis
                    type="number"
                    tick={{ fontSize: 11, fill: "#71717a" }}
                    tickFormatter={(v: number) => `${formatIndian(v)} Cr`}
                  />
                  <YAxis
                    type="category"
                    dataKey="name"
                    tick={{ fontSize: 11, fill: "#71717a" }}
                    width={90}
                  />
                  <Tooltip
                    contentStyle={{
                      borderRadius: "8px",
                      fontSize: "12px",
                      border: "1px solid #e4e4e7",
                    }}
                    // eslint-disable-next-line @typescript-eslint/no-explicit-any
                    formatter={(value: any) => [
                      `${formatIndian(Number(value))} Cr`,
                      "Est. Revenue",
                    ]}
                  />
                  <Bar
                    dataKey="revenue"
                    fill="#3b82f6"
                    radius={[0, 6, 6, 0]}
                    barSize={24}
                  />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>

        {/* Revenue Detail Table */}
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle>
                OEM Revenue Estimates — {fyQuarter}
              </CardTitle>
              <div className="flex items-center gap-1 text-xs text-zinc-400">
                <Info className="h-3 w-3" />
                All figures in Cr
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-zinc-200 text-left dark:border-zinc-800">
                    <th className="pb-3 pr-4 font-medium text-zinc-500">#</th>
                    <th className="pb-3 pr-4 font-medium text-zinc-500">OEM</th>
                    <th className="pb-3 px-2 font-medium text-zinc-500 text-center">Ticker</th>
                    <th className="pb-3 px-2 font-medium text-zinc-500 text-right">Registrations</th>
                    <th className="pb-3 px-2 font-medium text-zinc-500 text-right">Est. Rev (Cr)</th>
                    <th className="pb-3 px-2 font-medium text-zinc-500 text-right">Range Low</th>
                    <th className="pb-3 px-2 font-medium text-zinc-500 text-right">Range High</th>
                    <th className="pb-3 pl-2 font-medium text-zinc-500 text-right">Completeness</th>
                  </tr>
                </thead>
                <tbody>
                  {data.map((row, idx) => (
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
                      <td className="py-3 px-2 text-right font-mono text-zinc-700 dark:text-zinc-300">
                        {formatIndian(row.reg_volume)}
                      </td>
                      <td className="py-3 px-2 text-right font-mono font-semibold text-zinc-900 dark:text-zinc-100">
                        {formatIndian(row.est_domestic_rev_cr)}
                      </td>
                      <td className="py-3 px-2 text-right font-mono text-zinc-500">
                        {formatIndian(row.est_rev_low_cr)}
                      </td>
                      <td className="py-3 px-2 text-right font-mono text-zinc-500">
                        {formatIndian(row.est_rev_high_cr)}
                      </td>
                      <td className="py-3 pl-2 text-right">
                        <CompletenessBar pct={row.data_completeness_pct} />
                      </td>
                    </tr>
                  ))}
                </tbody>

                {data.length > 0 && (
                  <tfoot>
                    <tr className="border-t-2 border-zinc-300 font-semibold dark:border-zinc-700">
                      <td className="pt-3 pr-4" />
                      <td className="pt-3 pr-4 text-zinc-900 dark:text-zinc-100">
                        Total
                      </td>
                      <td className="pt-3 px-2" />
                      <td className="pt-3 px-2 text-right font-mono">
                        {formatIndian(totalVolume)}
                      </td>
                      <td className="pt-3 px-2 text-right font-mono">
                        {formatIndian(totalRevCr)}
                      </td>
                      <td className="pt-3 px-2 text-right font-mono text-zinc-500">
                        {formatIndian(
                          data.reduce((s, r) => s + r.est_rev_low_cr, 0)
                        )}
                      </td>
                      <td className="pt-3 px-2 text-right font-mono text-zinc-500">
                        {formatIndian(
                          data.reduce((s, r) => s + r.est_rev_high_cr, 0)
                        )}
                      </td>
                      <td className="pt-3 pl-2" />
                    </tr>
                  </tfoot>
                )}
              </table>
            </div>

            {data.length === 0 && (
              <div className="py-12 text-center text-zinc-500">
                No revenue estimate data available. Run the ETL pipeline and
                ensure ASP assumptions are seeded.
              </div>
            )}
          </CardContent>
        </Card>

        {/* Methodology Note */}
        <Card>
          <CardHeader>
            <CardTitle>Methodology</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2 text-sm text-zinc-600 dark:text-zinc-400">
              <p>
                Revenue estimates are computed as: <strong>Registrations x Segment-Level ASP</strong>,
                where ASP (Average Selling Price) assumptions are maintained per segment (PV/CV/2W)
                and powertrain (ICE/EV) in the fact_asp_master table.
              </p>
              <p>
                The &quot;Range Low&quot; and &quot;Range High&quot; columns reflect ASP sensitivity bands
                (typically +/-15%) to account for mix uncertainty within each segment.
              </p>
              <p>
                Data completeness indicates what percentage of the quarter has data available.
                A partial quarter will show proportionally lower volumes and revenue.
              </p>
            </div>
          </CardContent>
        </Card>

        <p className="text-center text-xs text-zinc-400">
          Demand-based proxy using registrations x assumed ASPs. This is NOT
          accounting revenue. Source: VAHAN (MoRTH).
        </p>
      </div>
    </div>
  );
}

function CompletenessBar({ pct }: { pct: number }) {
  return (
    <div className="flex items-center gap-2">
      <div className="h-1.5 w-16 rounded-full bg-zinc-200 dark:bg-zinc-800">
        <div
          className={cn(
            "h-full rounded-full",
            pct >= 80
              ? "bg-emerald-500"
              : pct >= 50
                ? "bg-amber-500"
                : "bg-red-500"
          )}
          style={{ width: `${Math.min(pct, 100)}%` }}
        />
      </div>
      <span className="text-xs font-mono text-zinc-500">{pct}%</span>
    </div>
  );
}
