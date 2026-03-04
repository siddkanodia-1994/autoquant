"use client";

import { useEffect, useState } from "react";
import { Header } from "@/components/layout/header";
import { KPICard } from "@/components/ui/kpi-card";
import { DailyTrendChart } from "@/components/charts/daily-trend-chart";
import { SegmentDonut } from "@/components/charts/segment-donut";
import { LoadingSpinner } from "@/components/ui/loading";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { formatIndian, formatPct, segmentLabel } from "@/lib/utils";
import type { IndustryPulseData } from "@/lib/types";

export default function IndustryPulsePage() {
  const [data, setData] = useState<IndustryPulseData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch("/api/industry")
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
        <div className="text-center">
          <p className="text-lg font-semibold text-red-600">Error loading data</p>
          <p className="mt-1 text-sm text-zinc-500">{error}</p>
        </div>
      </div>
    );
  }
  if (!data) return null;

  return (
    <div>
      <Header
        title="Industry Pulse"
        subtitle="All-India vehicle registrations across PV, CV, and 2W segments"
        dataDate={data.dataFreshness}
      />

      <div className="space-y-6 p-6">
        {/* Row 1: KPI Cards */}
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
          <KPICard
            title="MTD Total"
            value={data.mtdTotal}
            yoy={data.mtdYoY}
          />
          <KPICard
            title="QTD Total"
            value={data.qtdTotal}
            yoy={data.qtdYoY}
          />
          <KPICard
            title="YTD Total"
            value={data.ytdTotal}
            yoy={data.ytdYoY}
          />
          <KPICard
            title="Latest Day"
            value={data.latestDayTotal}
            subtitle={data.latestDate}
          />
        </div>

        {/* Row 2: Daily Trend + Segment Donut */}
        <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
          <div className="lg:col-span-2">
            <DailyTrendChart data={data.dailySeries} />
          </div>
          <SegmentDonut data={data.segmentBreakdown} />
        </div>

        {/* Row 3: Segment Detail Table */}
        <Card>
          <CardHeader>
            <CardTitle>Segment Breakdown (MTD)</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-zinc-200 text-left dark:border-zinc-800">
                    <th className="pb-2 font-medium text-zinc-500">Segment</th>
                    <th className="pb-2 text-right font-medium text-zinc-500">Volume</th>
                    <th className="pb-2 text-right font-medium text-zinc-500">Share</th>
                  </tr>
                </thead>
                <tbody>
                  {data.segmentBreakdown.map((seg) => (
                    <tr
                      key={seg.segment}
                      className="border-b border-zinc-100 dark:border-zinc-800/50"
                    >
                      <td className="py-2.5 font-medium text-zinc-900 dark:text-zinc-100">
                        {segmentLabel(seg.segment)}{" "}
                        <span className="text-zinc-400">({seg.segment})</span>
                      </td>
                      <td className="py-2.5 text-right font-mono text-zinc-700 dark:text-zinc-300">
                        {formatIndian(seg.value)}
                      </td>
                      <td className="py-2.5 text-right font-mono text-zinc-700 dark:text-zinc-300">
                        {seg.percentage.toFixed(1)}%
                      </td>
                    </tr>
                  ))}
                  <tr className="font-semibold">
                    <td className="pt-2 text-zinc-900 dark:text-zinc-100">
                      Total
                    </td>
                    <td className="pt-2 text-right font-mono text-zinc-900 dark:text-zinc-100">
                      {formatIndian(data.mtdTotal)}
                    </td>
                    <td className="pt-2 text-right font-mono text-zinc-900 dark:text-zinc-100">
                      100.0%
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>

        {/* Data disclaimer */}
        <p className="text-center text-xs text-zinc-400">
          Source: VAHAN (Ministry of Road Transport &amp; Highways). Data
          reflects new vehicle registrations, not sales. Updated daily.
        </p>
      </div>
    </div>
  );
}
