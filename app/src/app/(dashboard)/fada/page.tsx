"use client";

import React, { useEffect, useState, useMemo, useRef } from "react";
import { Header } from "@/components/layout/header";
import { KPICard } from "@/components/ui/kpi-card";
import { LoadingSpinner } from "@/components/ui/loading";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { formatIndian, formatPct, cn } from "@/lib/utils";
import type {
  FADADashboardData,
  FADAMonthData,
  FADAOemRow,
  FADAFYData,
  FADAFYOemRow,
} from "@/lib/types";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell,
  LineChart, Line, CartesianGrid,
} from "recharts";
import { Info, ChevronDown, ChevronRight } from "lucide-react";

// ── Constants ──

const SEGMENT_META: Record<string, { label: string; color: string }> = {
  "2W": { label: "Two-Wheelers", color: "#10b981" },
  CV: { label: "Commercial Vehicles", color: "#f59e0b" },
  PV: { label: "Passenger Vehicles", color: "#3b82f6" },
};

const OEM_COLORS = [
  "#3b82f6", "#ef4444", "#10b981", "#f59e0b", "#8b5cf6",
  "#ec4899", "#06b6d4", "#84cc16", "#f97316", "#6366f1",
  "#14b8a6", "#e11d48", "#a855f7", "#0ea5e9", "#d97706",
];

type ViewMode = "monthly" | "fy";
type TrendMetric = "volume" | "yoy";
type TrendGranularity = "monthly" | "yearly";

// ── Helpers ──

function monthLabel(period: string): string {
  const [y, m] = period.split("-");
  const months = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  return `${months[parseInt(m)]} ${y}`;
}

function shortOEM(name: string): string {
  return name
    .replace(/ PVT\.? LTD\.?/gi, "")
    .replace(/ PRIVATE LIMITED/gi, "")
    .replace(/ LIMITED/gi, "")
    .replace(/ LTD/gi, "")
    .replace(/ INDIA/gi, "")
    .replace(/\s*\(.*?\)/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

// ── Main Page ──

export default function FADAPage() {
  const [data, setData] = useState<FADADashboardData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Controls
  const [selectedSegment, setSelectedSegment] = useState<string>("PV");
  const [viewMode, setViewMode] = useState<ViewMode>("monthly");
  const [selectedMonth, setSelectedMonth] = useState<string>("");
  const [selectedFY, setSelectedFY] = useState<string>("");

  // Table sort
  const [sortBy, setSortBy] = useState<"volume" | "yoy" | "share">("volume");

  // Line chart controls
  const [selectedOEMs, setSelectedOEMs] = useState<string[]>([]);
  const [trendMetric, setTrendMetric] = useState<TrendMetric>("volume");
  const [trendGranularity, setTrendGranularity] = useState<TrendGranularity>("monthly");

  // Group expansion state
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(new Set());
  const toggleGroup = (groupName: string) => {
    setExpandedGroups((prev) => {
      const next = new Set(prev);
      if (next.has(groupName)) next.delete(groupName);
      else next.add(groupName);
      return next;
    });
  };

  // Fetch data
  useEffect(() => {
    fetch("/api/fada")
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((d: FADADashboardData) => {
        setData(d);
        setSelectedMonth(d.latestMonth);
        setSelectedFY(d.latestFY);
        if (d.segments.includes("PV")) setSelectedSegment("PV");
        else if (d.segments.length > 0) setSelectedSegment(d.segments[0]);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  // Set default OEM selection when data or segment changes
  useEffect(() => {
    if (!data) return;
    const latest = data.monthlyData.find(
      (d) => d.report_period === data.latestMonth && d.segment === selectedSegment
    );
    if (latest) {
      const topOEMs = [...latest.oems]
        .sort((a, b) => b.volume - a.volume)
        .slice(0, 5)
        .map((o) => o.oem_name);
      setSelectedOEMs(topOEMs);
    }
  }, [data, selectedSegment]);

  // ── Computed Data ──

  const currentMonthData = useMemo(() => {
    if (!data) return null;
    return data.monthlyData.find(
      (d) => d.report_period === selectedMonth && d.segment === selectedSegment
    );
  }, [data, selectedMonth, selectedSegment]);

  const currentFYData = useMemo(() => {
    if (!data) return null;
    return data.fyData.find(
      (d) => d.fy === selectedFY && d.segment === selectedSegment
    );
  }, [data, selectedFY, selectedSegment]);

  // All OEMs in current segment (for multi-select)
  const allOEMsInSegment = useMemo(() => {
    if (!data) return [];
    const oemSet = new Set<string>();
    for (const m of data.monthlyData) {
      if (m.segment === selectedSegment) {
        for (const o of m.oems) oemSet.add(o.oem_name);
      }
    }
    const latest = data.monthlyData.find(
      (d) => d.report_period === data.latestMonth && d.segment === selectedSegment
    );
    const volMap: Record<string, number> = {};
    if (latest) for (const o of latest.oems) volMap[o.oem_name] = o.volume;
    return [...oemSet].sort((a, b) => (volMap[b] || 0) - (volMap[a] || 0));
  }, [data, selectedSegment]);

  // Sorted OEMs for table
  const sortedOems = useMemo(() => {
    const oems = viewMode === "monthly"
      ? [...(currentMonthData?.oems || [])]
      : (currentFYData?.oems || []).map((o) => ({ ...o, mom_pct: null as number | null }));
    switch (sortBy) {
      case "volume": return oems.sort((a, b) => b.volume - a.volume);
      case "yoy": return oems.sort((a, b) => (b.yoy_pct ?? -999) - (a.yoy_pct ?? -999));
      case "share": return oems.sort((a, b) => b.market_share_pct - a.market_share_pct);
      default: return oems;
    }
  }, [currentMonthData, currentFYData, sortBy, viewMode]);

  // Bar chart data (top 10)
  const chartData = useMemo(() => {
    const oems = viewMode === "monthly" ? currentMonthData?.oems : currentFYData?.oems;
    if (!oems) return [];
    return [...oems]
      .sort((a, b) => b.volume - a.volume)
      .slice(0, 10)
      .map((oem) => ({
        name: shortOEM(oem.oem_name).substring(0, 20),
        fullName: oem.oem_name,
        volume: oem.volume,
        share: oem.market_share_pct,
        yoy: oem.yoy_pct,
      }));
  }, [currentMonthData, currentFYData, viewMode]);

  // ── Line Chart Data ──
  const trendData = useMemo(() => {
    if (!data || selectedOEMs.length === 0) return [];

    if (trendGranularity === "monthly") {
      const months = data.monthlyData
        .filter((d) => d.segment === selectedSegment)
        .map((d) => d.report_period)
        .filter((v, i, a) => a.indexOf(v) === i)
        .sort();

      return months.map((period) => {
        const md = data.monthlyData.find(
          (d) => d.report_period === period && d.segment === selectedSegment
        );
        const point: Record<string, any> = { period, label: monthLabel(period) };
        for (const oem of selectedOEMs) {
          const oemRow = md?.oems.find((o) => o.oem_name === oem);
          point[oem] = trendMetric === "volume"
            ? (oemRow?.volume ?? null)
            : (oemRow?.yoy_pct ?? null);
        }
        return point;
      });
    } else {
      const fys = data.fyData
        .filter((d) => d.segment === selectedSegment)
        .map((d) => d.fy)
        .filter((v, i, a) => a.indexOf(v) === i)
        .sort();

      return fys.map((fy) => {
        const fd = data.fyData.find(
          (d) => d.fy === fy && d.segment === selectedSegment
        );
        const point: Record<string, any> = { period: fy, label: fy };
        for (const oem of selectedOEMs) {
          const oemRow = fd?.oems.find((o) => o.oem_name === oem);
          point[oem] = trendMetric === "volume"
            ? (oemRow?.volume ?? null)
            : (oemRow?.yoy_pct ?? null);
        }
        return point;
      });
    }
  }, [data, selectedSegment, selectedOEMs, trendMetric, trendGranularity]);

  // ── KPI Computations ──
  const kpiData = useMemo(() => {
    if (viewMode === "monthly" && currentMonthData) {
      const md = currentMonthData;
      const nonOthers = md.oems.filter((o) => o.yoy_pct != null);
      const topGainer = nonOthers.length > 0
        ? nonOthers.reduce((a, b) => ((a.yoy_pct ?? -Infinity) > (b.yoy_pct ?? -Infinity) ? a : b))
        : null;
      const topLoser = nonOthers.length > 0
        ? nonOthers.reduce((a, b) => ((a.yoy_pct ?? Infinity) < (b.yoy_pct ?? Infinity) ? a : b))
        : null;
      return {
        totalVol: md.total_volume,
        totalYoY: md.total_yoy_pct,
        totalMoM: md.total_mom_pct,
        oemCount: md.oems.length,
        topGainer,
        topLoser: topLoser && topLoser.yoy_pct != null && topLoser.yoy_pct < 0 ? topLoser : null,
        avgMonthly: null as number | null,
        avgMonthlyYoY: null as number | null,
        monthsCount: null as number | null,
      };
    } else if (viewMode === "fy" && currentFYData) {
      const fd = currentFYData;
      const nonOthers = fd.oems.filter((o) => o.yoy_pct != null);
      const topGainer = nonOthers.length > 0
        ? nonOthers.reduce((a, b) => ((a.yoy_pct ?? -Infinity) > (b.yoy_pct ?? -Infinity) ? a : b))
        : null;
      const topLoser = nonOthers.length > 0
        ? nonOthers.reduce((a, b) => ((a.yoy_pct ?? Infinity) < (b.yoy_pct ?? Infinity) ? a : b))
        : null;
      return {
        totalVol: fd.total_volume,
        totalYoY: fd.total_yoy_pct,
        totalMoM: null,
        oemCount: fd.oems.length,
        topGainer,
        topLoser: topLoser && topLoser.yoy_pct != null && topLoser.yoy_pct < 0 ? topLoser : null,
        avgMonthly: fd.avg_monthly,
        avgMonthlyYoY: fd.avg_monthly_yoy_pct,
        monthsCount: fd.months_count,
      };
    }
    return null;
  }, [viewMode, currentMonthData, currentFYData]);

  // ── Render ──
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

  const segMeta = SEGMENT_META[selectedSegment] || { label: selectedSegment, color: "#6366f1" };
  const periodLabel = viewMode === "monthly" ? monthLabel(selectedMonth) : selectedFY;

  return (
    <div>
      <Header
        title="FADA Retail Data"
        subtitle="Monthly vehicle retail registrations by OEM — Source: FADA (via MoRTH RTO data)"
        dataDate={selectedMonth + "-01"}
      />

      <div className="space-y-6 p-6">
        {/* Info Banner */}
        <div className="flex items-start gap-3 rounded-xl border border-blue-200 bg-blue-50 p-4 dark:border-blue-900 dark:bg-blue-950/30">
          <Info className="mt-0.5 h-4 w-4 shrink-0 text-blue-500" />
          <div className="text-sm text-blue-800 dark:text-blue-300">
            <span className="font-semibold">Historical Data:</span> FADA retail registration data from April 2021 to Feb 2026 across {data.segments.length} segments.
            Indian Financial Year (FY) runs April to March — e.g., FY25 = Apr 2024 – Mar 2025.
          </div>
        </div>

        {/* Controls Row */}
        <div className="flex flex-wrap items-center gap-3">
          {/* Segment Tabs */}
          <div className="flex gap-1 rounded-lg bg-zinc-100 p-1 dark:bg-zinc-900">
            {data.segments
              .filter((s) => ["2W", "CV", "PV"].includes(s))
              .map((seg) => (
                <button
                  key={seg}
                  onClick={() => setSelectedSegment(seg)}
                  className={cn(
                    "rounded-md px-4 py-2 text-sm font-medium transition-all",
                    seg === selectedSegment
                      ? "bg-white shadow-sm text-zinc-900 dark:bg-zinc-800 dark:text-zinc-100"
                      : "text-zinc-500 hover:text-zinc-700 dark:text-zinc-400 dark:hover:text-zinc-200"
                  )}
                >
                  {SEGMENT_META[seg]?.label || seg}
                </button>
              ))}
          </div>

          {/* View Toggle */}
          <div className="flex gap-1 rounded-lg bg-zinc-100 p-1 dark:bg-zinc-900">
            {(["monthly", "fy"] as const).map((mode) => (
              <button
                key={mode}
                onClick={() => setViewMode(mode)}
                className={cn(
                  "rounded-md px-3 py-2 text-sm font-medium transition-all",
                  viewMode === mode
                    ? "bg-white shadow-sm text-zinc-900 dark:bg-zinc-800 dark:text-zinc-100"
                    : "text-zinc-500 hover:text-zinc-700 dark:text-zinc-400"
                )}
              >
                {mode === "monthly" ? "Monthly" : "Financial Year"}
              </button>
            ))}
          </div>

          {/* Period Picker */}
          {viewMode === "monthly" ? (
            <select
              value={selectedMonth}
              onChange={(e) => setSelectedMonth(e.target.value)}
              className="rounded-lg border border-zinc-200 bg-white px-3 py-2 text-sm text-zinc-700 shadow-sm dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-300"
            >
              {data.availableMonths.map((m) => (
                <option key={m} value={m}>{monthLabel(m)}</option>
              ))}
            </select>
          ) : (
            <select
              value={selectedFY}
              onChange={(e) => setSelectedFY(e.target.value)}
              className="rounded-lg border border-zinc-200 bg-white px-3 py-2 text-sm text-zinc-700 shadow-sm dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-300"
            >
              {data.availableFYs.map((fy) => (
                <option key={fy} value={fy}>
                  {fy}{fy === data.latestFY ? " (Partial)" : ""}
                </option>
              ))}
            </select>
          )}
        </div>

        {/* KPI Row */}
        {kpiData && (
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <KPICard
              title={`${segMeta.label} — Total`}
              value={kpiData.totalVol}
              yoy={kpiData.totalYoY ?? undefined}
              subtitle={viewMode === "fy" && kpiData.monthsCount
                ? `${kpiData.monthsCount} months of data`
                : undefined}
            />
            {viewMode === "monthly" ? (
              <KPICard
                title="MoM Change"
                value={kpiData.totalMoM != null ? `${kpiData.totalMoM > 0 ? "+" : ""}${kpiData.totalMoM.toFixed(1)}%` : "\u2014"}
                subtitle={periodLabel}
              />
            ) : (
              <KPICard
                title="Avg Monthly Volume"
                value={kpiData.avgMonthly ?? 0}
                yoy={kpiData.avgMonthlyYoY ?? undefined}
                subtitle={`${selectedFY} average`}
              />
            )}
            {kpiData.topGainer && (
              <KPICard
                title="Top Gainer"
                value={shortOEM(kpiData.topGainer.oem_name)}
                yoy={kpiData.topGainer.yoy_pct ?? undefined}
                subtitle={formatIndian(kpiData.topGainer.volume) + " units"}
              />
            )}
            {kpiData.topLoser && (
              <KPICard
                title="Biggest Decliner"
                value={shortOEM(kpiData.topLoser.oem_name)}
                yoy={kpiData.topLoser.yoy_pct ?? undefined}
                subtitle={formatIndian(kpiData.topLoser.volume) + " units"}
              />
            )}
          </div>
        )}

        {/* Bar Chart: Top 10 OEMs */}
        <Card>
          <CardHeader>
            <CardTitle>
              Top 10 OEMs — {segMeta.label} ({periodLabel})
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-[350px]">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chartData} layout="vertical" margin={{ left: 150, right: 40, top: 10, bottom: 10 }}>
                  <XAxis type="number" tickFormatter={(v) => formatIndian(v)} tick={{ fontSize: 11, fill: "#71717a" }} />
                  <YAxis type="category" dataKey="name" tick={{ fontSize: 11, fill: "#71717a" }} width={140} />
                  <Tooltip
                    formatter={(value: any) => [Number(value).toLocaleString("en-IN"), "Volume"]}
                    labelFormatter={(label: any) => {
                      const item = chartData.find((d) => d.name === label);
                      return item?.fullName || String(label);
                    }}
                    contentStyle={{ borderRadius: "8px", border: "1px solid #e4e4e7", fontSize: "12px" }}
                  />
                  <Bar dataKey="volume" radius={[0, 4, 4, 0]}>
                    {chartData.map((_, idx) => (
                      <Cell key={idx} fill={idx === 0 ? segMeta.color : `${segMeta.color}99`} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>

        {/* OEM Retail Volume Trends — Interactive Line Chart */}
        <Card>
          <CardHeader>
            <CardTitle>OEM Retail Volume Trends</CardTitle>
          </CardHeader>
          <CardContent>
            {/* Trend Controls */}
            <div className="mb-4 flex flex-wrap items-center gap-3">
              {/* OEM Multi-Select */}
              <OEMMultiSelect
                allOEMs={allOEMsInSegment}
                selected={selectedOEMs}
                onChange={setSelectedOEMs}
              />

              {/* Granularity Toggle */}
              <div className="flex gap-1 rounded-lg bg-zinc-100 p-0.5 dark:bg-zinc-800">
                {(["monthly", "yearly"] as const).map((g) => (
                  <button
                    key={g}
                    onClick={() => setTrendGranularity(g)}
                    className={cn(
                      "rounded-md px-3 py-1.5 text-xs font-medium transition-all",
                      trendGranularity === g
                        ? "bg-white shadow-sm text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100"
                        : "text-zinc-500 hover:text-zinc-700 dark:text-zinc-400"
                    )}
                  >
                    {g === "monthly" ? "Monthly" : "Yearly (FY)"}
                  </button>
                ))}
              </div>

              {/* Metric Toggle */}
              <div className="flex gap-1 rounded-lg bg-zinc-100 p-0.5 dark:bg-zinc-800">
                {(["volume", "yoy"] as const).map((m) => (
                  <button
                    key={m}
                    onClick={() => setTrendMetric(m)}
                    className={cn(
                      "rounded-md px-3 py-1.5 text-xs font-medium transition-all",
                      trendMetric === m
                        ? "bg-white shadow-sm text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100"
                        : "text-zinc-500 hover:text-zinc-700 dark:text-zinc-400"
                    )}
                  >
                    {m === "volume" ? "Volume" : "YoY %"}
                  </button>
                ))}
              </div>
            </div>

            {/* Line Chart */}
            <div className="h-[400px]">
              {trendData.length > 0 && selectedOEMs.length > 0 ? (
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={trendData} margin={{ left: 20, right: 20, top: 10, bottom: 10 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#e4e4e7" />
                    <XAxis
                      dataKey="label"
                      tick={{ fontSize: 10, fill: "#71717a" }}
                      interval={trendGranularity === "monthly" ? Math.max(0, Math.floor(trendData.length / 12)) : 0}
                      angle={-30}
                      textAnchor="end"
                      height={50}
                    />
                    <YAxis
                      tick={{ fontSize: 11, fill: "#71717a" }}
                      tickFormatter={(v) =>
                        trendMetric === "volume" ? formatIndian(v) : `${v}%`
                      }
                    />
                    <Tooltip
                      contentStyle={{ borderRadius: "8px", border: "1px solid #e4e4e7", fontSize: "12px" }}
                      formatter={(value: any, name: any) => {
                        if (value == null) return ["\u2014", shortOEM(name)];
                        return [
                          trendMetric === "volume"
                            ? Number(value).toLocaleString("en-IN")
                            : `${Number(value).toFixed(1)}%`,
                          shortOEM(name),
                        ];
                      }}
                      labelFormatter={(label: any) => String(label)}
                    />
                    {selectedOEMs.map((oem, idx) => (
                      <Line
                        key={oem}
                        type="monotone"
                        dataKey={oem}
                        stroke={OEM_COLORS[idx % OEM_COLORS.length]}
                        strokeWidth={2}
                        dot={false}
                        activeDot={{ r: 4 }}
                        connectNulls
                        name={oem}
                      />
                    ))}
                  </LineChart>
                </ResponsiveContainer>
              ) : (
                <div className="flex h-full items-center justify-center text-sm text-zinc-400">
                  Select OEMs above to view trends
                </div>
              )}
            </div>

            {/* Legend */}
            {selectedOEMs.length > 0 && (
              <div className="mt-3 flex flex-wrap gap-3">
                {selectedOEMs.map((oem, idx) => (
                  <div key={oem} className="flex items-center gap-1.5 text-xs text-zinc-600 dark:text-zinc-400">
                    <div
                      className="h-2.5 w-2.5 rounded-full"
                      style={{ backgroundColor: OEM_COLORS[idx % OEM_COLORS.length] }}
                    />
                    {shortOEM(oem)}
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        {/* OEM Detail Table */}
        <Card>
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle>
              OEM-wise Retail Data — {segMeta.label} ({periodLabel})
            </CardTitle>
            <div className="flex gap-1 rounded-lg bg-zinc-100 p-0.5 dark:bg-zinc-800">
              {([["volume", "Volume"], ["share", "Share"], ["yoy", "YoY"]] as const).map(([key, label]) => (
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
                    <th className="pb-2 text-right font-medium text-zinc-500">Volume</th>
                    <th className="pb-2 text-right font-medium text-zinc-500">Share</th>
                    <th className="pb-2 text-right font-medium text-zinc-500">YoY</th>
                    {viewMode === "monthly" && (
                      <th className="pb-2 text-right font-medium text-zinc-500">MoM</th>
                    )}
                  </tr>
                </thead>
                <tbody>
                  {sortedOems.map((oem, idx) => {
                    const isGroup = !!(oem as any).is_group;
                    const children = (oem as any).children as typeof sortedOems | undefined;
                    const isExpanded = expandedGroups.has(oem.oem_name);

                    return (
                      <React.Fragment key={oem.oem_name}>
                        <tr
                          className={cn(
                            "border-b border-zinc-100 dark:border-zinc-800/50",
                            isGroup && "cursor-pointer hover:bg-zinc-50 dark:hover:bg-zinc-800/50"
                          )}
                          onClick={isGroup ? () => toggleGroup(oem.oem_name) : undefined}
                        >
                          <td className="py-2.5 pl-1 text-zinc-400 font-mono text-xs">{idx + 1}</td>
                          <td className="py-2.5 font-medium text-zinc-900 dark:text-zinc-100">
                            <span className="flex items-center gap-1.5">
                              {isGroup && (
                                <span className="inline-flex h-4 w-4 shrink-0 items-center justify-center text-zinc-400">
                                  {isExpanded ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
                                </span>
                              )}
                              {oem.oem_name}
                              {isGroup && children && (
                                <span className="ml-1 rounded-full bg-zinc-100 px-1.5 py-0.5 text-[10px] font-normal text-zinc-500 dark:bg-zinc-800 dark:text-zinc-400">
                                  {children.length} sub
                                </span>
                              )}
                            </span>
                          </td>
                          <td className="py-2.5 text-right font-mono text-zinc-700 dark:text-zinc-300">
                            {oem.volume.toLocaleString("en-IN")}
                          </td>
                          <td className="py-2.5 text-right font-mono text-zinc-700 dark:text-zinc-300">
                            {oem.market_share_pct.toFixed(1)}%
                          </td>
                          <td className="py-2.5 text-right">
                            <GrowthBadge value={oem.yoy_pct} />
                          </td>
                          {viewMode === "monthly" && (
                            <td className="py-2.5 text-right">
                              <GrowthBadge value={"mom_pct" in oem ? (oem as any).mom_pct : null} />
                            </td>
                          )}
                        </tr>
                        {/* Expanded children rows */}
                        {isGroup && isExpanded && children?.map((child) => (
                          <tr
                            key={`${oem.oem_name}__${child.oem_name}`}
                            className="border-b border-zinc-50 bg-zinc-50/50 dark:border-zinc-800/30 dark:bg-zinc-900/30"
                          >
                            <td className="py-2 pl-1"></td>
                            <td className="py-2 pl-7 text-sm text-zinc-600 dark:text-zinc-400">
                              {child.oem_name}
                            </td>
                            <td className="py-2 text-right font-mono text-xs text-zinc-500 dark:text-zinc-400">
                              {child.volume.toLocaleString("en-IN")}
                            </td>
                            <td className="py-2 text-right font-mono text-xs text-zinc-500 dark:text-zinc-400">
                              {child.market_share_pct.toFixed(1)}%
                            </td>
                            <td className="py-2 text-right">
                              <GrowthBadge value={child.yoy_pct} />
                            </td>
                            {viewMode === "monthly" && (
                              <td className="py-2 text-right">
                                <GrowthBadge value={"mom_pct" in child ? (child as any).mom_pct : null} />
                              </td>
                            )}
                          </tr>
                        ))}
                      </React.Fragment>
                    );
                  })}
                  {/* Total Row */}
                  <tr className="font-semibold">
                    <td className="pt-3"></td>
                    <td className="pt-3 text-zinc-900 dark:text-zinc-100">TOTAL</td>
                    <td className="pt-3 text-right font-mono text-zinc-900 dark:text-zinc-100">
                      {(viewMode === "monthly" ? currentMonthData?.total_volume : currentFYData?.total_volume)?.toLocaleString("en-IN") ?? "\u2014"}
                    </td>
                    <td className="pt-3 text-right font-mono text-zinc-900 dark:text-zinc-100">100.0%</td>
                    <td className="pt-3 text-right">
                      <GrowthBadge value={kpiData?.totalYoY} bold />
                    </td>
                    {viewMode === "monthly" && (
                      <td className="pt-3 text-right">
                        <GrowthBadge value={kpiData?.totalMoM} bold />
                      </td>
                    )}
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
          reflects vehicle retail registrations at RTOs. Historical data: April 2021 — Feb 2026.
        </p>
      </div>
    </div>
  );
}

// ── Sub-Components ──

function GrowthBadge({ value, bold }: { value: number | null | undefined; bold?: boolean }) {
  if (value == null) return <span className="text-xs text-zinc-400">{"\u2014"}</span>;
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs",
        bold ? "font-semibold" : "font-medium",
        value > 0
          ? "bg-emerald-50 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-400"
          : value < 0
            ? "bg-red-50 text-red-700 dark:bg-red-950 dark:text-red-400"
            : "bg-zinc-100 text-zinc-600 dark:bg-zinc-800 dark:text-zinc-400"
      )}
    >
      {formatPct(value)}
    </span>
  );
}

function OEMMultiSelect({
  allOEMs,
  selected,
  onChange,
}: {
  allOEMs: string[];
  selected: string[];
  onChange: (v: string[]) => void;
}) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  // Close on outside click
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const toggle = (oem: string) => {
    if (selected.includes(oem)) {
      onChange(selected.filter((s) => s !== oem));
    } else if (selected.length < 10) {
      onChange([...selected, oem]);
    }
  };

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen(!open)}
        className="flex items-center gap-2 rounded-lg border border-zinc-200 bg-white px-3 py-1.5 text-xs font-medium text-zinc-700 shadow-sm hover:bg-zinc-50 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-300 dark:hover:bg-zinc-800"
      >
        {selected.length} OEMs selected
        <ChevronDown className="h-3 w-3" />
      </button>

      {open && (
        <div className="absolute left-0 top-full z-50 mt-1 max-h-72 w-72 overflow-y-auto rounded-xl border border-zinc-200 bg-white p-2 shadow-xl dark:border-zinc-700 dark:bg-zinc-900">
          <div className="mb-2 flex items-center justify-between px-1">
            <span className="text-xs text-zinc-500">{selected.length}/10 selected</span>
            <button
              onClick={() => onChange([])}
              className="text-xs text-blue-500 hover:text-blue-700"
            >
              Clear all
            </button>
          </div>
          {allOEMs.map((oem) => (
            <label
              key={oem}
              className="flex cursor-pointer items-center gap-2 rounded-md px-2 py-1.5 text-xs hover:bg-zinc-50 dark:hover:bg-zinc-800"
            >
              <input
                type="checkbox"
                checked={selected.includes(oem)}
                onChange={() => toggle(oem)}
                className="h-3.5 w-3.5 rounded border-zinc-300 text-blue-600 focus:ring-blue-500"
              />
              <span className="truncate text-zinc-700 dark:text-zinc-300">
                {shortOEM(oem)}
              </span>
            </label>
          ))}
        </div>
      )}
    </div>
  );
}
