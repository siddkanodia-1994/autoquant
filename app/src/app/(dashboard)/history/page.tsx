/**
 * /history — Historical Data Coverage (2016–Present)
 *
 * Shows:
 *   - Data coverage progress bar & KPI cards
 *   - Year-by-year summary table with confidence badges
 *   - Monthly volume area chart with source color-coding
 *   - EV adoption timeline
 *   - Data source legend & methodology
 */

import { fetchHistoricalData } from "@/lib/queries";
import { Header } from "@/components/layout/header";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { KPICard } from "@/components/ui/kpi-card";
import { formatIndian } from "@/lib/utils";
import { HistoricalCharts } from "./charts";

export const revalidate = 3600;

export default async function HistoryPage() {
  const data = await fetchHistoricalData();

  const totalVolume = data.yearSummaries.reduce(
    (sum, y) => sum + y.total_registrations,
    0
  );
  const yearsSpanned = data.yearSummaries.length;
  const latestYear =
    data.yearSummaries.length > 0
      ? data.yearSummaries[data.yearSummaries.length - 1]
      : null;

  const highConfCount = data.yearSummaries.filter(
    (y) => y.confidence === "HIGH"
  ).length;
  const medConfCount = data.yearSummaries.filter(
    (y) => y.confidence === "MEDIUM"
  ).length;
  const lowConfCount = data.yearSummaries.filter(
    (y) => y.confidence === "LOW"
  ).length;

  return (
    <div className="space-y-6">
      <Header title="Historical Data" />

      {/* Data Coverage Banner */}
      <div className="rounded-lg border border-blue-200 bg-blue-50 p-4 dark:border-blue-900 dark:bg-blue-950/30">
        <div className="flex items-center justify-between mb-2">
          <h3 className="text-sm font-semibold text-blue-800 dark:text-blue-300">
            Data Coverage: Jan 2016 &rarr; Present
          </h3>
          <span className="text-sm font-mono text-blue-700 dark:text-blue-400">
            {data.coveragePct.toFixed(1)}%
          </span>
        </div>
        <div className="h-2.5 w-full rounded-full bg-blue-100 dark:bg-blue-900/50">
          <div
            className="h-2.5 rounded-full bg-blue-600 transition-all"
            style={{ width: `${Math.min(100, data.coveragePct)}%` }}
          />
        </div>
        <p className="mt-2 text-xs text-blue-600 dark:text-blue-400">
          {data.monthlyTrend.length} months of data across{" "}
          {data.dataRange.min || "—"} to {data.dataRange.max || "—"}
        </p>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <KPICard
          title="Total Volume (All Time)"
          value={formatIndian(totalVolume)}
          subtitle={`${yearsSpanned} years of data`}
        />
        <KPICard
          title="Latest Year"
          value={latestYear ? formatIndian(latestYear.total_registrations) : "—"}
          subtitle={latestYear ? `${latestYear.year} registrations` : "No data"}
          trend={latestYear?.yoy_pct ?? undefined}
        />
        <KPICard
          title="EV Penetration (Latest)"
          value={latestYear ? `${latestYear.ev_pct.toFixed(1)}%` : "—"}
          subtitle={
            latestYear ? `${formatIndian(latestYear.ev_registrations)} EVs` : ""
          }
        />
        <KPICard
          title="Confidence"
          value={`${highConfCount}H / ${medConfCount}M / ${lowConfCount}L`}
          subtitle="High / Medium / Low years"
        />
      </div>

      {/* Interactive Charts (Client Component) */}
      <HistoricalCharts
        monthlyTrend={data.monthlyTrend}
        yearSummaries={data.yearSummaries}
      />

      {/* Year-by-Year Table */}
      <Card>
        <CardHeader>
          <CardTitle>Year-by-Year Summary</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-zinc-200 text-left text-xs font-medium uppercase tracking-wider text-zinc-500 dark:border-zinc-700">
                  <th className="px-3 py-2">Year</th>
                  <th className="px-3 py-2">FY</th>
                  <th className="px-3 py-2 text-right">Total</th>
                  <th className="px-3 py-2 text-right">PV</th>
                  <th className="px-3 py-2 text-right">CV</th>
                  <th className="px-3 py-2 text-right">2W</th>
                  <th className="px-3 py-2 text-right">EV %</th>
                  <th className="px-3 py-2 text-right">YoY</th>
                  <th className="px-3 py-2 text-center">Months</th>
                  <th className="px-3 py-2 text-center">OEMs</th>
                  <th className="px-3 py-2 text-center">Source</th>
                  <th className="px-3 py-2 text-center">Confidence</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-zinc-100 dark:divide-zinc-800">
                {data.yearSummaries.map((year) => (
                  <tr
                    key={year.year}
                    className="hover:bg-zinc-50 dark:hover:bg-zinc-900"
                  >
                    <td className="px-3 py-2.5 font-medium text-zinc-900 dark:text-zinc-100">
                      {year.year}
                    </td>
                    <td className="px-3 py-2.5 text-zinc-500">
                      {year.fy_label}
                    </td>
                    <td className="px-3 py-2.5 text-right font-mono text-zinc-900 dark:text-zinc-100">
                      {formatIndian(year.total_registrations)}
                    </td>
                    <td className="px-3 py-2.5 text-right font-mono text-blue-600">
                      {formatIndian(year.pv_registrations)}
                    </td>
                    <td className="px-3 py-2.5 text-right font-mono text-amber-600">
                      {formatIndian(year.cv_registrations)}
                    </td>
                    <td className="px-3 py-2.5 text-right font-mono text-emerald-600">
                      {formatIndian(year.tw_registrations)}
                    </td>
                    <td className="px-3 py-2.5 text-right font-mono">
                      {year.ev_pct > 0 ? `${year.ev_pct.toFixed(1)}%` : "—"}
                    </td>
                    <td className="px-3 py-2.5 text-right">
                      <YoYBadge value={year.yoy_pct} />
                    </td>
                    <td className="px-3 py-2.5 text-center">
                      <MonthCoverage months={year.months_with_data} />
                    </td>
                    <td className="px-3 py-2.5 text-center text-zinc-600 dark:text-zinc-400">
                      {year.oems_with_data}
                    </td>
                    <td className="px-3 py-2.5 text-center">
                      <SourceBadge source={year.data_source} />
                    </td>
                    <td className="px-3 py-2.5 text-center">
                      <ConfidenceBadge level={year.confidence} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>

      {/* Data Source Legend */}
      <Card>
        <CardHeader>
          <CardTitle>Data Sources & Confidence</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4 text-sm text-zinc-600 dark:text-zinc-400">
          <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
            <div className="rounded-lg border border-green-200 bg-green-50 p-3 dark:border-green-900 dark:bg-green-950/30">
              <div className="flex items-center gap-2 mb-1">
                <span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-green-500 text-[10px] font-bold text-white">
                  H
                </span>
                <span className="font-semibold text-green-800 dark:text-green-300">
                  High Confidence
                </span>
              </div>
              <p className="text-xs text-green-700 dark:text-green-400">
                Live VAHAN daily feed. Individual vehicle-level registration
                data aggregated daily. Near real-time with 1-2 day lag.
              </p>
            </div>
            <div className="rounded-lg border border-amber-200 bg-amber-50 p-3 dark:border-amber-900 dark:bg-amber-950/30">
              <div className="flex items-center gap-2 mb-1">
                <span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-amber-500 text-[10px] font-bold text-white">
                  M
                </span>
                <span className="font-semibold text-amber-800 dark:text-amber-300">
                  Medium Confidence
                </span>
              </div>
              <p className="text-xs text-amber-700 dark:text-amber-400">
                SIAM historical data with 10+ months coverage. Monthly
                aggregates from industry body publications. May have
                interpolated monthly estimates.
              </p>
            </div>
            <div className="rounded-lg border border-red-200 bg-red-50 p-3 dark:border-red-900 dark:bg-red-950/30">
              <div className="flex items-center gap-2 mb-1">
                <span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-red-500 text-[10px] font-bold text-white">
                  L
                </span>
                <span className="font-semibold text-red-800 dark:text-red-300">
                  Low Confidence
                </span>
              </div>
              <p className="text-xs text-red-700 dark:text-red-400">
                Sparse historical data with fewer than 10 months coverage.
                May contain estimation gaps or incomplete OEM representation.
                Use directional trends only.
              </p>
            </div>
          </div>

          <div className="border-t border-zinc-200 pt-3 dark:border-zinc-700">
            <h4 className="font-semibold text-zinc-700 dark:text-zinc-300 mb-2">
              Methodology Notes
            </h4>
            <ul className="space-y-1 text-xs text-zinc-500 dark:text-zinc-400 list-disc list-inside">
              <li>
                Historical data (2016-2024) sourced from SIAM annual
                statistical profiles and category-wise domestic sales reports.
              </li>
              <li>
                Live data (2025+) from VAHAN (Ministry of Road Transport &
                Highways) daily registration feeds.
              </li>
              <li>
                EV registrations tracked separately from 2020 onwards; earlier
                years may undercount EV volumes.
              </li>
              <li>
                Tata Motors split into PV and CV entities from Jan 2025
                (demerger). Pre-2025 data allocated by vehicle class mapping.
              </li>
              <li>
                Calendar year shown; FY label reflects the dominant Financial
                Year (e.g., CY2025 ≈ FY26).
              </li>
            </ul>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

// ── Sub-components ──

function ConfidenceBadge({ level }: { level: string }) {
  const styles: Record<string, string> = {
    HIGH: "bg-green-100 text-green-800 dark:bg-green-900/50 dark:text-green-300",
    MEDIUM:
      "bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-300",
    LOW: "bg-red-100 text-red-800 dark:bg-red-900/50 dark:text-red-300",
  };

  return (
    <span
      className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-semibold ${
        styles[level] || styles.LOW
      }`}
    >
      {level}
    </span>
  );
}

function SourceBadge({ source }: { source: string }) {
  const isVahan = source.includes("VAHAN");
  const isSiam = source.includes("SIAM");

  if (isVahan && isSiam) {
    return (
      <span className="inline-flex items-center gap-1">
        <span className="rounded bg-blue-100 px-1.5 py-0.5 text-[9px] font-medium text-blue-700 dark:bg-blue-900/50 dark:text-blue-400">
          VAHAN
        </span>
        <span className="rounded bg-zinc-100 px-1.5 py-0.5 text-[9px] font-medium text-zinc-600 dark:bg-zinc-800 dark:text-zinc-400">
          SIAM
        </span>
      </span>
    );
  }

  if (isVahan) {
    return (
      <span className="rounded bg-blue-100 px-1.5 py-0.5 text-[9px] font-medium text-blue-700 dark:bg-blue-900/50 dark:text-blue-400">
        VAHAN
      </span>
    );
  }

  return (
    <span className="rounded bg-zinc-100 px-1.5 py-0.5 text-[9px] font-medium text-zinc-600 dark:bg-zinc-800 dark:text-zinc-400">
      SIAM
    </span>
  );
}

function YoYBadge({ value }: { value: number | null }) {
  if (value === null) return <span className="text-zinc-400">—</span>;

  const isPositive = value > 0;
  const color = isPositive
    ? "text-green-600 dark:text-green-400"
    : "text-red-600 dark:text-red-400";

  return (
    <span className={`font-mono text-xs ${color}`}>
      {isPositive ? "+" : ""}
      {value.toFixed(1)}%
    </span>
  );
}

function MonthCoverage({ months }: { months: number }) {
  const bars = Array.from({ length: 12 }, (_, i) => i < months);

  return (
    <div className="flex items-center gap-[2px] justify-center" title={`${months}/12 months`}>
      {bars.map((filled, i) => (
        <div
          key={i}
          className={`h-3 w-1.5 rounded-sm ${
            filled
              ? "bg-blue-500 dark:bg-blue-400"
              : "bg-zinc-200 dark:bg-zinc-700"
          }`}
        />
      ))}
    </div>
  );
}
