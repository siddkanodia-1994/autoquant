"use client";

import { useEffect, useState, useMemo } from "react";
import { Header } from "@/components/layout/header";
import { KPICard } from "@/components/ui/kpi-card";
import { LoadingSpinner } from "@/components/ui/loading";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { formatIndian, formatPct, cn } from "@/lib/utils";
import type { FADADashboardData, FADAMonthData, FADAOemRow } from "@/lib/types";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from "recharts";

const SEGMENT_META: Record<string, { label: string; color: string }> = {
  "2W": { label: "Two-Wheelers", color: "#10b981" },
  CV: { label: "Commercial Vehicles", color: "#f59e0b" },
  PV: { label: "Passenger Vehicles", color: "#3b82f6" },
};

function monthLabel(period: string): string {
  const [y, m] = period.split("-");
  const months = [
    "", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
  ];
  return `${months[parseInt(m)]} ${y}`;
}

export default function FADAPage() {
  const [data, setData] = useState<FADADashboardData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedSegment, setSelectedSegment] = useState<string>("PV");
  const [selectedMonth, setSelectedMonth] = useState<string>("");
  const [sortBy, setSortBy] = useState<"volume" | "yoy" | "share">("volume");

  useEffect(() => {
    fetch("/api/fada")
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((d: FADADashboardData) => {
        setData(d);
        setSelectedMonth(d.latestMonth);
        if (d.segments.includes("PV")) setSelectedSegment("PV");
        else if (d.segments.length > 0) setSelectedSegment(d.segments[0]);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  // Current month data for selected segment
  const currentData = useMemo(() => {
    if (!data) return null;
    return data.data.find(
      (d) => d.report_period === selectedMonth && d.segment === selectedSegment
    );
  }, [data, selectedMonth, selectedSegment]);

  // Prior year same month for YoY comparison
  const priorYearData = useMemo(() => {
    if (!data || !selectedMonth) return null;
    const [y, m] = selectedMonth.split("-");
    const priorPeriod = `${Number(y) - 1}-${m}`;
    return data.data.find(
      (d) => d.report_period === priorPeriod && d.segment === selectedSegment
    );
  }, [data, selectedMonth, selectedSegment]);

  // Sorted OEMs
  const sortedOems = useMemo(() => {
    if (!currentData) return [];
    const oems = [...currentData.oems];
    switch (sortBy) {
      case "volume":
        return oems.sort((a, b) => b.volume - a.volume);
      case "yoy":
        return oems.sort((a, b) => (b.yoy_pct ?? -999) - (a.yoy_pct ?? -999));
      case "share":
        return oems.sort((a, b) => b.market_share_pct - a.market_share_pct);
      default:
        return oems;
    }
  }, [currentData, sortBy]);

  // Chart data (top 10 OEMs by volume)
  const chartData = useMemo(() => {
    if (!currentData) return [];
    return [...currentData.oems]
      .sort((a, b) => b.volume - a.volume)
      .slice(0, 10)
      .map((oem) => ({
        name: oem.oem_name.length > 18
          ? oem.oem_name.substring(0, 16) + "…"
          : oem.oem_name,
        fullName: oem.oem_name,
        volume: oem.volume,
        share: oem.market_share_pct,
        yoy: oem.yoy_pct,
      }));
  }, [currentData]);

  if (loading) return <LoadingSpinner className="h-screen" />;
  if (error) {
    return (
      <div className="flex h-screen items-center justify-center">
        <div className="text-center">
          <p className="text-lg font-semibold text-red-600">Error loading data</p>
          <p className="mt-1 text-sm text-zinc-500">{error}</p>
        </div>
      </div>
    );
  }
  if (!data) return null;

  const segMeta = SEGMENT_META[selectedSegment] || {
    label: selectedSegment,
    color: "#6366f1",
  };

  const totalVol = currentData?.total_volume ?? 0;
  const priorVol = priorYearData?.total_volume ?? 0;
  const totalYoY = priorVol > 0 ? ((totalVol - priorVol) / priorVol) * 100 : null;
  const oemCount = currentData?.oems.filter((o) => o.oem_name !== "OTHERS").length ?? 0;

  // Top gainer / loser
  const nonOthers = currentData?.oems.filter(
    (o) => o.oem_name !== "OTHERS" && o.yoy_pct != null
  ) || [];
  const topGainer = nonOthers.length > 0
    ? nonOthers.reduce((a, b) => ((a.yoy_pct ?? -Infinity) > (b.yoy_pct ?? -Infinity) ? a : b))
    : null;
  const topLoser = nonOthers.length > 0
    ? nonOthers.reduce((a, b) => ((a.yoy_pct ?? Infinity) < (b.yoy_pct ?? Infinity) ? a : b))
    : null;

  return (
    <div>
      <Header
        title="FADA Retail Data"
        subtitle="Monthly vehicle retail registrations by OEM — Source: FADA (via MoRTH RTO data)"
        dataDate={selectedMonth + "-01"}
      />

      <div className="space-y-6 p-6">
        {/* Controls: Segment Tabs + Month Selector */}
        <div className="flex flex-wrap items-center justify-between gap-4">
          {/* Segment Tabs */}
          <div className="flex gap-1 rounded-lg bg-zinc-100 p-1 dark:bg-zinc-900">
            {data.segments
              .filter((s) => ["2W", "CV", "PV"].includes(s))
              .map((seg) => {
                const meta = SEGMENT_META[seg];
                const isActive = seg === selectedSegment;
                return (
                  <button
                    key={seg}
                    onClick={() => setSelectedSegment(seg)}
                    className={cn(
                      "rounded-md px-4 py-2 text-sm font-medium transition-all",
                      isActive
                        ? "bg-white shadow-sm text-zinc-900 dark:bg-zinc-800 dark:text-zinc-100"
                        : "text-zinc-500 hover:text-zinc-700 dark:text-zinc-400 dark:hover:text-zinc-200"
                    )}
                  >
                    {meta?.label || seg}
                  </button>
                );
              })}
          </div>

          {/* Month Selector */}
          <select
            value={selectedMonth}
            onChange={(e) => setSelectedMonth(e.target.value)}
            className="rounded-lg border border-zinc-200 bg-white px-3 py-2 text-sm text-zinc-700 shadow-sm dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-300"
          >
            {data.availableMonths.map((m) => (
              <option key={m} value={m}>
                {monthLabel(m)}
              </option>
            ))}
          </select>
        </div>

        {/* KPI Row */}
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
          <KPICard
            title={`${segMeta.label} — Total`}
            value={totalVol}
            yoy={totalYoY ?? undefined}
          />
          <KPICard
            title="OEMs Tracked"
            value={oemCount}
            subtitle={`${monthLabel(selectedMonth)}`}
          />
          {topGainer && (
            <KPICard
              title="Top Gainer"
              value={topGainer.oem_name.split(" ").slice(0, 2).join(" ")}
              yoy={topGainer.yoy_pct ?? undefined}
              subtitle={formatIndian(topGainer.volume) + " units"}
            />
          )}
          {topLoser && topLoser.yoy_pct != null && topLoser.yoy_pct < 0 && (
            <KPICard
              title="Biggest Decliner"
              value={topLoser.oem_name.split(" ").slice(0, 2).join(" ")}
              yoy={topLoser.yoy_pct ?? undefined}
              subtitle={formatIndian(topLoser.volume) + " units"}
            />
          )}
        </div>

        {/* Bar Chart: Top 10 OEMs */}
        <Card>
          <CardHeader>
            <CardTitle>
              Top 10 OEMs — {segMeta.label} ({monthLabel(selectedMonth)})
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-[350px]">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart
                  data={chartData}
                  layout="vertical"
                  margin={{ left: 140, right: 40, top: 10, bottom: 10 }}
                >
                  <XAxis
                    type="number"
                    tickFormatter={(v) => formatIndian(v)}
                    tick={{ fontSize: 11, fill: "#71717a" }}
                  />
                  <YAxis
                    type="category"
                    dataKey="name"
                    tick={{ fontSize: 11, fill: "#71717a" }}
                    width={130}
                  />
                  <Tooltip
                    formatter={(value: any) => [
                      Number(value).toLocaleString("en-IN"),
                      "Volume",
                    ]}
                    labelFormatter={(label: any) => {
                      const item = chartData.find((d) => d.name === label);
                      return item?.fullName || String(label);
                    }}
                    contentStyle={{
                      borderRadius: "8px",
                      border: "1px solid #e4e4e7",
                      fontSize: "12px",
                    }}
                  />
                  <Bar dataKey="volume" radius={[0, 4, 4, 0]}>
                    {chartData.map((_, idx) => (
                      <Cell
                        key={idx}
                        fill={idx === 0 ? segMeta.color : `${segMeta.color}99`}
                      />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>

        {/* OEM Detail Table */}
        <Card>
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle>
              OEM-wise Retail Data — {segMeta.label}
            </CardTitle>
            <div className="flex gap-1 rounded-lg bg-zinc-100 p-0.5 dark:bg-zinc-800">
              {(
                [
                  ["volume", "Volume"],
                  ["share", "Share"],
                  ["yoy", "YoY"],
                ] as const
              ).map(([key, label]) => (
                <button
                  key={key}
                  onClick={() => setSortBy(key)}
                  className={cn(
                    "rounded-md px-3 py-1 text-xs font-medium transition-all",
                    sortBy === key
                      ? "bg-white shadow-sm text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100"
                      : "text-zinc-500 hover:text-zinc-700 dark:text-zinc-400"
                  )}
                >
                  {label}
                </button>
              ))}
            </div>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-zinc-200 text-left dark:border-zinc-800">
                    <th className="pb-2 pl-1 font-medium text-zinc-500">#</th>
                    <th className="pb-2 font-medium text-zinc-500">OEM</th>
                    <th className="pb-2 text-right font-medium text-zinc-500">
                      Volume
                    </th>
                    <th className="pb-2 text-right font-medium text-zinc-500">
                      Market Share
                    </th>
                    <th className="pb-2 text-right font-medium text-zinc-500">
                      YoY Growth
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {sortedOems.map((oem, idx) => {
                    const isOthers = oem.oem_name === "OTHERS";
                    return (
                      <tr
                        key={oem.oem_name}
                        className={cn(
                          "border-b border-zinc-100 dark:border-zinc-800/50",
                          isOthers && "bg-zinc-50/50 dark:bg-zinc-900/30"
                        )}
                      >
                        <td className="py-2.5 pl-1 text-zinc-400 font-mono text-xs">
                          {idx + 1}
                        </td>
                        <td className="py-2.5 font-medium text-zinc-900 dark:text-zinc-100">
                          {oem.oem_name}
                        </td>
                        <td className="py-2.5 text-right font-mono text-zinc-700 dark:text-zinc-300">
                          {oem.volume.toLocaleString("en-IN")}
                        </td>
                        <td className="py-2.5 text-right font-mono text-zinc-700 dark:text-zinc-300">
                          {oem.market_share_pct.toFixed(1)}%
                        </td>
                        <td className="py-2.5 text-right">
                          {oem.yoy_pct != null ? (
                            <span
                              className={cn(
                                "inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium",
                                oem.yoy_pct > 0
                                  ? "bg-emerald-50 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-400"
                                  : oem.yoy_pct < 0
                                    ? "bg-red-50 text-red-700 dark:bg-red-950 dark:text-red-400"
                                    : "bg-zinc-100 text-zinc-600 dark:bg-zinc-800 dark:text-zinc-400"
                              )}
                            >
                              {formatPct(oem.yoy_pct)}
                            </span>
                          ) : (
                            <span className="text-xs text-zinc-400">—</span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                  {/* Total Row */}
                  <tr className="font-semibold">
                    <td className="pt-3"></td>
                    <td className="pt-3 text-zinc-900 dark:text-zinc-100">
                      TOTAL
                    </td>
                    <td className="pt-3 text-right font-mono text-zinc-900 dark:text-zinc-100">
                      {totalVol.toLocaleString("en-IN")}
                    </td>
                    <td className="pt-3 text-right font-mono text-zinc-900 dark:text-zinc-100">
                      100.0%
                    </td>
                    <td className="pt-3 text-right">
                      {totalYoY != null && (
                        <span
                          className={cn(
                            "inline-flex items-center rounded-full px-2 py-0.5 text-xs font-semibold",
                            totalYoY > 0
                              ? "bg-emerald-50 text-emerald-700"
                              : "bg-red-50 text-red-700"
                          )}
                        >
                          {formatPct(totalYoY)}
                        </span>
                      )}
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>

        {/* Disclaimer */}
        <p className="text-center text-xs text-zinc-400">
          Source: Federation of Automobile Dealers Associations (FADA), in
          collaboration with Ministry of Road Transport &amp; Highways. Data
          reflects vehicle retail registrations at RTOs.
        </p>
      </div>
    </div>
  );
}
