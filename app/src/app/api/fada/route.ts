/**
 * GET /api/fada
 * Returns FADA monthly OEM retail data across 2W, CV, PV segments.
 */
import { NextResponse } from "next/server";
import { fetchFADAData } from "@/lib/queries";

// Force dynamic to avoid build cache reusing stale pre-rendered data.
// CDN-level caching via Cache-Control handles performance.
export const dynamic = "force-dynamic";

export async function GET() {
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
