/**
 * GET /api/oem
 * Returns list of in-scope listed OEMs.
 * Includes provenance metadata.
 */
import { NextResponse } from "next/server";
import { fetchOEMList } from "@/lib/queries";
import { vahanMeta, GENERAL_DISCLAIMER } from "@/lib/metadata";

export const revalidate = 86400; // Revalidate daily (OEM list rarely changes)

export async function GET() {
  try {
    const data = await fetchOEMList();
    return NextResponse.json({
      ...data,
      _meta: {
        ...vahanMeta("24h"),
        disclaimer: GENERAL_DISCLAIMER,
      },
    });
  } catch (error) {
    console.error("[API] /api/oem error:", error);
    return NextResponse.json(
      { error: "Failed to fetch OEM list" },
      { status: 500 }
    );
  }
}
