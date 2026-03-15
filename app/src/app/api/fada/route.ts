/**
 * GET /api/fada
 * Returns FADA monthly OEM retail data across 2W, CV, PV segments.
 */
import { NextResponse } from "next/server";
import { fetchFADAData } from "@/lib/queries";

// Force dynamic so Supabase is queried live (not from stale build cache).
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
// OTHERS data added: 2026-03-15T07:27:46Z
