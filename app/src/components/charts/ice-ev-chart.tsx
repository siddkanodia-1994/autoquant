"use client";

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { POWERTRAIN_COLORS, formatIndian } from "@/lib/utils";
import type { ICEvsEVPoint } from "@/lib/types";

interface ICEvsEVChartProps {
  data: ICEvsEVPoint[];
  title?: string;
}

export function ICEvsEVChart({
  data,
  title = "ICE vs EV Monthly Volume",
}: ICEvsEVChartProps) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={data}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e4e4e7" />
              <XAxis
                dataKey="month"
                tick={{ fontSize: 11, fill: "#71717a" }}
                tickFormatter={(m: string) => m.slice(2)} // YY-MM
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
                  String(name).toUpperCase(),
                ]}
              />
              <Legend
                wrapperStyle={{ fontSize: "12px" }}
                formatter={(value: unknown) => String(value).toUpperCase()}
              />
              <Bar
                dataKey="ice"
                stackId="a"
                fill={POWERTRAIN_COLORS.ICE}
                radius={[0, 0, 0, 0]}
              />
              <Bar
                dataKey="ev"
                stackId="a"
                fill={POWERTRAIN_COLORS.EV}
                radius={[4, 4, 0, 0]}
              />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
