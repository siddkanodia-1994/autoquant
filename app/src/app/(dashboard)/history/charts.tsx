"use client";

/**
 * Historical Charts — Client Component
 *
 * 1. Monthly volume stacked area chart (PV / CV / 2W) with source demarcation
 * 2. Annual EV adoption bar chart
 */

import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  BarChart,
  Bar,
  ReferenceLine,
  Cell,
} from "recharts";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { SEGMENT_COLORS } from "@/lib/utils";
import type {
  HistoricalMonthPoint,
  HistoricalYearSummary,
} from "@/lib/types";

interface HistoricalChartsProps {
  monthlyTrend: HistoricalMonthPoint[];
  yearSummaries: HistoricalYearSummary[];
}

export function HistoricalCharts({
  monthlyTrend,
  yearSummaries,
}: HistoricalChartsProps) {
  // Format month labels for x-axis: show only Jan of each year
  const formatMonth = (tick: string) => {
    if (!tick) return "";
    const [year, month] = tick.split("-");
    if (month === "01") return year;
    return "";
  };

  // Format large numbers for y-axis
  const formatYAxis = (value: number) => {
    if (value >= 10_000_000) return `${(value / 10_000_000).toFixed(0)}Cr`;
    if (value >= 100_000) return `${(value / 100_000).toFixed(0)}L`;
    if (value >= 1_000) return `${(value / 1_000).toFixed(0)}K`;
    return value.toString();
  };

  // Find the boundary between SIAM and VAHAN data
  const vahanStart = monthlyTrend.findIndex((m) => m.source === "VAHAN");
  const vahanStartMonth =
    vahanStart >= 0 ? monthlyTrend[vahanStart].month : null;

  return (
    <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
      {/* Monthly Volume Stacked Area */}
      <Card className="lg:col-span-2">
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle>Monthly Registration Volume (2016–Present)</CardTitle>
            <div className="flex items-center gap-4 text-xs">
              <span className="flex items-center gap-1">
                <span
                  className="inline-block h-2.5 w-2.5 rounded-sm"
                  style={{ background: SEGMENT_COLORS.PV }}
                />
                PV
              </span>
              <span className="flex items-center gap-1">
                <span
                  className="inline-block h-2.5 w-2.5 rounded-sm"
                  style={{ background: SEGMENT_COLORS.CV }}
                />
                CV
              </span>
              <span className="flex items-center gap-1">
                <span
                  className="inline-block h-2.5 w-2.5 rounded-sm"
                  style={{ background: SEGMENT_COLORS["2W"] }}
                />
                2W
              </span>
              {vahanStartMonth && (
                <span className="flex items-center gap-1 text-zinc-400">
                  <span className="inline-block h-2.5 w-px bg-red-400" />
                  VAHAN live start
                </span>
              )}
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <ResponsiveContainer width="100%" height={340}>
            <AreaChart
              data={monthlyTrend}
              margin={{ top: 5, right: 20, left: 10, bottom: 5 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#e4e4e7" />
              <XAxis
                dataKey="month"
                tickFormatter={formatMonth}
                tick={{ fontSize: 11 }}
                interval="preserveStartEnd"
                minTickGap={40}
              />
              <YAxis
                tickFormatter={formatYAxis}
                tick={{ fontSize: 11 }}
                width={55}
              />
              <Tooltip
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                formatter={(value: any, name: any) => [
                  Number(value).toLocaleString("en-IN"),
                  String(name) === "tw"
                    ? "2W"
                    : String(name).toUpperCase(),
                ]}
                labelFormatter={(label: unknown) => {
                  const s = String(label);
                  const [y, m] = s.split("-");
                  const months = [
                    "",
                    "Jan",
                    "Feb",
                    "Mar",
                    "Apr",
                    "May",
                    "Jun",
                    "Jul",
                    "Aug",
                    "Sep",
                    "Oct",
                    "Nov",
                    "Dec",
                  ];
                  return `${months[parseInt(m)] || m} ${y}`;
                }}
              />
              {vahanStartMonth && (
                <ReferenceLine
                  x={vahanStartMonth}
                  stroke="#ef4444"
                  strokeDasharray="4 4"
                  label={{
                    value: "VAHAN Live",
                    position: "top",
                    fill: "#ef4444",
                    fontSize: 10,
                  }}
                />
              )}
              <Area
                type="monotone"
                dataKey="tw"
                stackId="1"
                stroke={SEGMENT_COLORS["2W"]}
                fill={SEGMENT_COLORS["2W"]}
                fillOpacity={0.6}
              />
              <Area
                type="monotone"
                dataKey="cv"
                stackId="1"
                stroke={SEGMENT_COLORS.CV}
                fill={SEGMENT_COLORS.CV}
                fillOpacity={0.6}
              />
              <Area
                type="monotone"
                dataKey="pv"
                stackId="1"
                stroke={SEGMENT_COLORS.PV}
                fill={SEGMENT_COLORS.PV}
                fillOpacity={0.6}
              />
            </AreaChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>

      {/* Annual Totals Bar Chart */}
      <Card>
        <CardHeader>
          <CardTitle>Annual Registrations</CardTitle>
        </CardHeader>
        <CardContent>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart
              data={yearSummaries}
              margin={{ top: 5, right: 10, left: 10, bottom: 5 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#e4e4e7" />
              <XAxis dataKey="year" tick={{ fontSize: 11 }} />
              <YAxis
                tickFormatter={formatYAxis}
                tick={{ fontSize: 11 }}
                width={55}
              />
              <Tooltip
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                formatter={(value: any) => [
                  Number(value).toLocaleString("en-IN"),
                  "Registrations",
                ]}
              />
              <Bar dataKey="total_registrations" radius={[4, 4, 0, 0]}>
                {yearSummaries.map((entry) => (
                  <Cell
                    key={entry.year}
                    fill={
                      entry.confidence === "HIGH"
                        ? "#3b82f6"
                        : entry.confidence === "MEDIUM"
                          ? "#f59e0b"
                          : "#94a3b8"
                    }
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          <div className="mt-2 flex items-center justify-center gap-4 text-xs text-zinc-500">
            <span className="flex items-center gap-1">
              <span className="inline-block h-2.5 w-2.5 rounded-sm bg-blue-500" />
              High confidence
            </span>
            <span className="flex items-center gap-1">
              <span className="inline-block h-2.5 w-2.5 rounded-sm bg-amber-500" />
              Medium
            </span>
            <span className="flex items-center gap-1">
              <span className="inline-block h-2.5 w-2.5 rounded-sm bg-slate-400" />
              Low
            </span>
          </div>
        </CardContent>
      </Card>

      {/* EV Adoption Timeline */}
      <Card>
        <CardHeader>
          <CardTitle>EV Adoption Over Time</CardTitle>
        </CardHeader>
        <CardContent>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart
              data={yearSummaries.filter((y) => y.ev_registrations > 0)}
              margin={{ top: 5, right: 10, left: 10, bottom: 5 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#e4e4e7" />
              <XAxis dataKey="year" tick={{ fontSize: 11 }} />
              <YAxis
                yAxisId="left"
                tickFormatter={formatYAxis}
                tick={{ fontSize: 11 }}
                width={55}
              />
              <YAxis
                yAxisId="right"
                orientation="right"
                tickFormatter={(v: number) => `${v.toFixed(0)}%`}
                tick={{ fontSize: 11 }}
                width={45}
                domain={[0, "auto"]}
              />
              <Tooltip
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                formatter={(value: any, name: any) => {
                  const n = String(name);
                  if (n === "ev_registrations")
                    return [Number(value).toLocaleString("en-IN"), "EV Units"];
                  return [`${Number(value).toFixed(1)}%`, "EV Share"];
                }}
              />
              <Bar
                yAxisId="left"
                dataKey="ev_registrations"
                fill="#22c55e"
                radius={[4, 4, 0, 0]}
                fillOpacity={0.8}
              />
              <Bar
                yAxisId="right"
                dataKey="ev_pct"
                fill="#86efac"
                radius={[4, 4, 0, 0]}
                fillOpacity={0.4}
              />
            </BarChart>
          </ResponsiveContainer>
          <div className="mt-2 flex items-center justify-center gap-4 text-xs text-zinc-500">
            <span className="flex items-center gap-1">
              <span className="inline-block h-2.5 w-2.5 rounded-sm bg-green-500" />
              EV registrations
            </span>
            <span className="flex items-center gap-1">
              <span className="inline-block h-2.5 w-2.5 rounded-sm bg-green-300" />
              EV share %
            </span>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
