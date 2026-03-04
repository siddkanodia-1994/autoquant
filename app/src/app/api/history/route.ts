/**
 * GET /api/history — Historical data spanning 2016-present.
 *
 * Returns year summaries with confidence indicators,
 * monthly time series, and data coverage metrics.
 * Includes historical data disclaimer and provenance metadata.
 *
 * Revalidates every hour (ISR).
 */

import { NextResponse } from "next/server";
import { fetchHistoricalData } from "@/lib/queries";
import { vahanMeta, HISTORICAL_DISCLAIMER, GENERAL_DISCLAIMER } from "@/lib/metadata";

export const revalidate = 3600; // 1 hour

export async function GET() {
  try {
    const data = await fetchHistoricalData();
    return NextResponse.json({
      ...data,
      _meta: {
        ...vahanMeta("1h"),
        disclaimer: HISTORICAL_DISCLAIMER,
        general_disclaimer: GENERAL_DISCLAIMER,
      },
    });
  } catch (error) {
    console.error("Historical data fetch error:", error);
    return NextResponse.json(
      { error: "Failed to fetch historical data" },
      { status: 500 }
    );
  }
}
