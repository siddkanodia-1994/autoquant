/**
 * GET /api/scorecard
 * Returns OEM scorecard: aggregated volumes, YoY, market share.
 * Includes provenance metadata and data disclaimer.
 */
import { NextResponse } from "next/server";
import { fetchScorecard } from "@/lib/queries";
import { vahanMeta, GENERAL_DISCLAIMER } from "@/lib/metadata";

export const revalidate = 3600;

export async function GET() {
  try {
    const data = await fetchScorecard();
    return NextResponse.json({
      ...data,
      _meta: {
        ...vahanMeta("1h"),
        disclaimer: GENERAL_DISCLAIMER,
      },
    });
  } catch (error) {
    console.error("[API] /api/scorecard error:", error);
    return NextResponse.json(
      { error: "Failed to fetch scorecard data" },
      { status: 500 }
    );
  }
}
