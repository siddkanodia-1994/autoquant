"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { Header } from "@/components/layout/header";
import { KPICard } from "@/components/ui/kpi-card";
import { ICEvsEVChart } from "@/components/charts/ice-ev-chart";
import { LoadingSpinner } from "@/components/ui/loading";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { formatIndian, formatPct } from "@/lib/utils";
import { ArrowLeft, ExternalLink } from "lucide-react";
import type { OEMDeepDiveData } from "@/lib/types";

export default function OEMDeepDivePage() {
  const params = useParams();
  const ticker = params.ticker as string;

  const [data, setData] = useState<OEMDeepDiveData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!ticker) return;
    fetch(`/api/oem/${ticker}`)
      .then((r) => {
        if (r.status === 404) throw new Error("OEM not found");
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(setData)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [ticker]);

  if (loading) return <LoadingSpinner className="h-screen" />;
  if (error) {
    return (
      <div className="flex h-screen flex-col items-center justify-center gap-4">
        <p className="text-lg font-semibold text-red-600">{error}</p>
        <Link
          href="/oem"
          className="text-sm text-blue-600 hover:underline"
        >
          Back to OEM list
        </Link>
      </div>
    );
  }
  if (!data) return null;

  const { oem } = data;

  return (
    <div>
      <Header
        title={oem.oem_name}
        subtitle={`NSE: ${oem.nse_ticker || "—"} · BSE: ${oem.bse_code || "—"}`}
      />

      <div className="space-y-6 p-6">
        {/* Back link */}
        <Link
          href="/oem"
          className="inline-flex items-center gap-1 text-sm text-zinc-500 hover:text-zinc-900 dark:hover:text-zinc-100 transition-colors"
        >
          <ArrowLeft className="h-3.5 w-3.5" />
          All OEMs
        </Link>

        {/* KPI Row */}
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
          <KPICard
            title="MTD Volume"
            value={data.mtdVolume}
            yoy={data.mtdYoY}
          />
          <KPICard
            title="QTD Volume"
            value={data.qtdVolume}
            yoy={data.qtdYoY}
          />
          <KPICard
            title="YTD Volume"
            value={data.ytdVolume}
            yoy={data.ytdYoY}
          />
          <KPICard
            title="MoM Change"
            value={data.mtdVolume}
            yoy={data.mtdMoM}
            subtitle="vs prev. month"
          />
        </div>

        {/* Charts Row */}
        <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
          {/* ICE vs EV */}
          <ICEvsEVChart
            data={data.iceEvSplit}
            title={`${oem.oem_name} — ICE vs EV`}
          />

          {/* OEM Info Card */}
          <Card>
            <CardHeader>
              <CardTitle>Company Details</CardTitle>
            </CardHeader>
            <CardContent>
              <dl className="space-y-3 text-sm">
                <div className="flex justify-between">
                  <dt className="text-zinc-500">NSE Ticker</dt>
                  <dd className="font-medium text-zinc-900 dark:text-zinc-100">
                    {oem.nse_ticker || "—"}
                  </dd>
                </div>
                <div className="flex justify-between">
                  <dt className="text-zinc-500">BSE Code</dt>
                  <dd className="font-medium text-zinc-900 dark:text-zinc-100">
                    {oem.bse_code || "—"}
                  </dd>
                </div>
                <div className="flex justify-between">
                  <dt className="text-zinc-500">Listed</dt>
                  <dd className="font-medium text-zinc-900 dark:text-zinc-100">
                    {oem.is_listed ? "Yes" : "No"}
                  </dd>
                </div>
                <div className="flex justify-between">
                  <dt className="text-zinc-500">Primary Segments</dt>
                  <dd className="font-medium text-zinc-900 dark:text-zinc-100">
                    {oem.primary_segments?.join(", ") || "—"}
                  </dd>
                </div>
              </dl>

              {oem.nse_ticker && (
                <div className="mt-6">
                  <a
                    href={`https://www.nseindia.com/get-quotes/equity?symbol=${oem.nse_ticker}`}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-1 text-sm text-blue-600 hover:underline"
                  >
                    View on NSE <ExternalLink className="h-3 w-3" />
                  </a>
                </div>
              )}
            </CardContent>
          </Card>
        </div>

        {/* Market Share Trend — placeholder for future */}
        {data.marketShareTrend.length > 0 && (
          <Card>
            <CardHeader>
              <CardTitle>Market Share Trend</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-zinc-500">
                Market share trend chart will be available once sufficient
                historical data is loaded.
              </p>
            </CardContent>
          </Card>
        )}

        {/* Disclaimer */}
        <p className="text-center text-xs text-zinc-400">
          Source: VAHAN (MoRTH). Registrations ≠ sales. Data reflects new
          vehicle registrations only.
        </p>
      </div>
    </div>
  );
}
