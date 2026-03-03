/**
 * GET /api/oem/[ticker]
 * Returns OEM Deep Dive data for a specific NSE ticker.
 */
import { NextResponse } from "next/server";
import { fetchOEMDeepDive } from "@/lib/queries";

export const revalidate = 3600;

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ ticker: string }> }
) {
  try {
    const { ticker } = await params;
    const data = await fetchOEMDeepDive(ticker.toUpperCase());

    if (!data) {
      return NextResponse.json(
        { error: `OEM with ticker "${ticker}" not found` },
        { status: 404 }
      );
    }

    return NextResponse.json(data);
  } catch (error) {
    console.error("[API] /api/oem/[ticker] error:", error);
    return NextResponse.json(
      { error: "Failed to fetch OEM data" },
      { status: 500 }
    );
  }
}
