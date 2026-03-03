/**
 * POST /api/revalidate
 * On-demand ISR revalidation triggered by ETL after data load.
 * Requires REVALIDATION_SECRET in Authorization header.
 */
import { NextResponse } from "next/server";
import { revalidatePath } from "next/cache";

export async function POST(request: Request) {
  const secret = process.env.REVALIDATION_SECRET;

  // Validate authorization
  const authHeader = request.headers.get("Authorization");
  if (!secret || authHeader !== `Bearer ${secret}`) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  try {
    const body = await request.json().catch(() => ({}));
    const paths = body.paths || ["/dashboard", "/oem"];

    for (const path of paths) {
      revalidatePath(path);
    }

    return NextResponse.json({
      revalidated: true,
      paths,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    console.error("[API] /api/revalidate error:", error);
    return NextResponse.json(
      { error: "Revalidation failed" },
      { status: 500 }
    );
  }
}
