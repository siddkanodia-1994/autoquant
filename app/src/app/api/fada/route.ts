/**
 * GET /api/fada
 * Returns FADA monthly OEM retail data across 2W, CV, PV segments.
 * Use ?debug=1 to get batch diagnostics.
 */
import { NextResponse } from "next/server";
import { NextRequest } from "next/server";
import { fetchFADAData } from "@/lib/queries";
import { supabase } from "@/lib/supabase";

// Force dynamic to avoid build cache reusing stale pre-rendered data.
export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const debug = request.nextUrl.searchParams.get("debug");

  // Debug mode: return batch diagnostics
  if (debug === "1") {
    const selectCols = "report_period, oem_name, segment, volume, yoy_pct";
    const baseQuery = () =>
      supabase
        .from("raw_fada_report")
        .select(selectCols)
        .eq("data_type", "actual")
        .order("report_period", { ascending: false })
        .order("segment")
        .order("volume", { ascending: false });

    const [res1, res2, res3] = await Promise.all([
      baseQuery().gte("report_period", "2025-06"),
      baseQuery().gte("report_period", "2024-01").lt("report_period", "2025-06"),
      baseQuery().lt("report_period", "2024-01"),
    ]);

    const allRows = [
      ...(res1.data || []),
      ...(res2.data || []),
      ...(res3.data || []),
    ];
    const months = [...new Set(allRows.map((r: any) => r.report_period))].sort();

    return NextResponse.json({
      batch1: { rows: res1.data?.length ?? 0, error: res1.error?.message ?? null },
      batch2: { rows: res2.data?.length ?? 0, error: res2.error?.message ?? null },
      batch3: { rows: res3.data?.length ?? 0, error: res3.error?.message ?? null },
      totalRows: allRows.length,
      distinctMonths: months.length,
      months,
      earliestMonth: months[0] ?? null,
      latestMonth: months[months.length - 1] ?? null,
    });
  }

  try {
    const data = await fetchFADAData();
    return NextResponse.json(data, {
      headers: {
        "Cache-Control": "public, s-maxage=3600, stale-while-revalidate=86400",
      },
    });
  } catch (error) {
    console.error("[API] /api/fada error:", error);
    return NextResponse.json(
      { error: "Failed to fetch FADA data" },
      { status: 500 }
    );
  }
}
