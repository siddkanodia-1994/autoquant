/**
 * GET /api/industry
 * Returns Industry Pulse data: MTD totals, segment breakdown, daily series.
 * Includes provenance metadata and data disclaimer.
 */
import { NextResponse } from "next/server";
import { fetchIndustryPulse } from "@/lib/queries";
import { vahanMeta, GENERAL_DISCLAIMER } from "@/lib/metadata";

export const revalidate = 3600; // ISR: revalidate every hour

export async function GET() {
  try {
    const data = await fetchIndustryPulse();
    return NextResponse.json({
      ...data,
      _meta: {
        ...vahanMeta("1h"),
        disclaimer: GENERAL_DISCLAIMER,
      },
    });
  } catch (error) {
    console.error("[API] /api/industry error:", error);
    return NextResponse.json(
      { error: "Failed to fetch industry data" },
      { status: 500 }
    );
  }
}
