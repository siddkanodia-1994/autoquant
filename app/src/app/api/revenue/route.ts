/**
 * GET /api/revenue
 * Returns demand-based revenue proxy estimates for listed OEMs.
 * Includes mandatory revenue disclaimer and provenance metadata.
 */
import { NextResponse } from "next/server";
import { fetchRevenueEstimates } from "@/lib/queries";
import { vahanMeta, REVENUE_DISCLAIMER } from "@/lib/metadata";

export const revalidate = 3600;

export async function GET() {
  try {
    const data = await fetchRevenueEstimates();
    return NextResponse.json({
      ...data,
      _meta: {
        ...vahanMeta("1h"),
        disclaimer: REVENUE_DISCLAIMER,
      },
    });
  } catch (error) {
    console.error("[API] /api/revenue error:", error);
    return NextResponse.json(
      { error: "Failed to fetch revenue estimates" },
      { status: 500 }
    );
  }
}
