"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { Header } from "@/components/layout/header";
import { LoadingSpinner } from "@/components/ui/loading";
import { Card, CardContent } from "@/components/ui/card";
import { Building2, ChevronRight } from "lucide-react";
import type { OEM } from "@/lib/types";

export default function OEMListPage() {
  const [oems, setOems] = useState<OEM[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/oem")
      .then((r) => r.json())
      .then(setOems)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <LoadingSpinner className="h-screen" />;

  return (
    <div>
      <Header
        title="OEM Deep Dive"
        subtitle="Select an OEM to view detailed registration data, EV mix, and market share"
      />

      <div className="p-6">
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {oems.map((oem) => (
            <Link
              key={oem.oem_id}
              href={`/oem/${oem.nse_ticker}`}
            >
              <Card className="transition-all hover:shadow-md hover:border-blue-300 dark:hover:border-blue-700 cursor-pointer">
                <CardContent className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-blue-50 dark:bg-blue-950">
                      <Building2 className="h-5 w-5 text-blue-600" />
                    </div>
                    <div>
                      <p className="font-semibold text-zinc-900 dark:text-zinc-100">
                        {oem.oem_name}
                      </p>
                      <p className="text-xs text-zinc-500">
                        {oem.nse_ticker || "—"}{" "}
                        {oem.primary_segments?.length > 0 &&
                          `· ${oem.primary_segments.join(", ")}`}
                      </p>
                    </div>
                  </div>
                  <ChevronRight className="h-4 w-4 text-zinc-400" />
                </CardContent>
              </Card>
            </Link>
          ))}
        </div>

        {oems.length === 0 && (
          <div className="py-20 text-center">
            <p className="text-zinc-500">
              No OEM data available yet. Run the ETL pipeline to populate data.
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
