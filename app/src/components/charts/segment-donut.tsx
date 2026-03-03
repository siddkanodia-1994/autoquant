"use client";

import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from "recharts";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { SEGMENT_COLORS, formatIndian } from "@/lib/utils";
import type { SegmentShare } from "@/lib/types";

interface SegmentDonutProps {
  data: SegmentShare[];
  title?: string;
}

export function SegmentDonut({
  data,
  title = "Segment Mix (MTD)",
}: SegmentDonutProps) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="h-52">
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie
                data={data}
                cx="50%"
                cy="50%"
                innerRadius={55}
                outerRadius={80}
                paddingAngle={3}
                dataKey="value"
                nameKey="segment"
              >
                {data.map((entry) => (
                  <Cell
                    key={entry.segment}
                    fill={SEGMENT_COLORS[entry.segment] || "#94a3b8"}
                  />
                ))}
              </Pie>
              <Tooltip
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                formatter={(value: any, name: any) => [
                  formatIndian(Number(value)),
                  String(name),
                ]}
                contentStyle={{
                  borderRadius: "8px",
                  fontSize: "12px",
                  border: "1px solid #e4e4e7",
                }}
              />
            </PieChart>
          </ResponsiveContainer>
        </div>
        <div className="mt-2 space-y-1.5">
          {data.map((item) => (
            <div
              key={item.segment}
              className="flex items-center justify-between text-sm"
            >
              <div className="flex items-center gap-2">
                <div
                  className="h-2.5 w-2.5 rounded-full"
                  style={{
                    backgroundColor:
                      SEGMENT_COLORS[item.segment] || "#94a3b8",
                  }}
                />
                <span className="text-zinc-600 dark:text-zinc-400">
                  {item.segment}
                </span>
              </div>
              <span className="font-medium text-zinc-900 dark:text-zinc-100">
                {item.percentage.toFixed(1)}%
              </span>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
