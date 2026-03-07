/**
 * GET /api/fada
 * Returns FADA monthly OEM retail data across 2W, CV, PV segments.
 */
import { NextResponse } from "next/server";
import { fetchFADAData } from "@/lib/queries";

export const revalidate = 3600;

export async function GET() {
  try {
    const data = await fetchFADAData();
    return NextResponse.json(data);
  } catch (error) {
    console.error("[API] /api/fada error:", error);
    return NextResponse.json(
      { error: "Failed to fetch FADA data" },
      { status: 500 }
    );
  }
}
