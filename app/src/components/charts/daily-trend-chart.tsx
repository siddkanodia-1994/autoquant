"use client";

import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Line,
  ComposedChart,
} from "recharts";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { SEGMENT_COLORS, formatIndian } from "@/lib/utils";
import type { DailyDataPoint } from "@/lib/types";

interface DailyTrendChartProps {
  data: DailyDataPoint[];
}

export function DailyTrendChart({ data }: DailyTrendChartProps) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Daily Registrations Trend</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="h-72">
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={data}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e4e4e7" />
              <XAxis
                dataKey="date"
                tick={{ fontSize: 11, fill: "#71717a" }}
                tickFormatter={(d: string) => d.slice(5)} // MM-DD
                interval="preserveStartEnd"
              />
              <YAxis
                tick={{ fontSize: 11, fill: "#71717a" }}
                tickFormatter={formatIndian}
              />
              <Tooltip
                contentStyle={{
                  borderRadius: "8px",
                  fontSize: "12px",
                  border: "1px solid #e4e4e7",
                }}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                formatter={(value: any, name: any) => [
                  formatIndian(Number(value)),
                  String(name) === "ma7" ? "7-day MA" : String(name).toUpperCase(),
                ]}
                labelFormatter={(label: unknown) => `Date: ${String(label)}`}
              />
              <Area
                type="monotone"
                dataKey="pv"
                stackId="1"
                fill={SEGMENT_COLORS.PV}
                fillOpacity={0.6}
                stroke={SEGMENT_COLORS.PV}
                name="pv"
              />
              <Area
                type="monotone"
                dataKey="cv"
                stackId="1"
                fill={SEGMENT_COLORS.CV}
                fillOpacity={0.6}
                stroke={SEGMENT_COLORS.CV}
                name="cv"
              />
              <Area
                type="monotone"
                dataKey="tw"
                stackId="1"
                fill={SEGMENT_COLORS["2W"]}
                fillOpacity={0.6}
                stroke={SEGMENT_COLORS["2W"]}
                name="2w"
              />
              <Line
                type="monotone"
                dataKey="ma7"
                stroke="#1e293b"
                strokeWidth={2}
                strokeDasharray="4 4"
                dot={false}
                name="ma7"
              />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
        <div className="mt-3 flex items-center justify-center gap-6">
          {Object.entries(SEGMENT_COLORS).map(([seg, color]) => (
            <div key={seg} className="flex items-center gap-1.5">
              <div
                className="h-2.5 w-2.5 rounded-full"
                style={{ backgroundColor: color }}
              />
              <span className="text-xs text-zinc-500">{seg}</span>
            </div>
          ))}
          <div className="flex items-center gap-1.5">
            <div className="h-0 w-4 border-t-2 border-dashed border-zinc-800" />
            <span className="text-xs text-zinc-500">7-day MA</span>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
