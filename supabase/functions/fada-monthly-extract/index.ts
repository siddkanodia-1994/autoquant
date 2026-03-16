import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";

// ── Constants ──
const FADA_BASE_URL = "https://www.fadaindia.org";
const FADA_STATS_URL = `${FADA_BASE_URL}/sales-statistics`;

const MONTH_NAMES = [
  "", "January", "February", "March", "April", "May", "June",
  "July", "August", "September", "October", "November", "December",
];

const FADA_SEGMENT_MAP: Record<string, string> = {
  "passenger vehicles": "PV",
  "passenger vehicle": "PV",
  "pv": "PV",
  "commercial vehicles": "CV",
  "commercial vehicle": "CV",
  "cv": "CV",
  "two wheelers": "2W",
  "two wheeler": "2W",
  "2w": "2W",
  "two-wheelers": "2W",
  "three wheelers": "3W",
  "three wheeler": "3W",
  "3w": "3W",
};

// Fuel-type aggregates to filter out (not real OEMs)
const NON_OEM_NAMES = new Set([
  "DIESEL", "EV", "PETROL/ETHANOL", "CNG/LPG", "HYBRID",
  "METHANOL", "OTHERS INCLUDING EV", "TOTAL", "GRAND TOTAL",
  "SUB TOTAL", "INDUSTRY_TOTAL",
]);

interface FADARecord {
  report_period: string;
  oem_name: string;
  segment: string;
  volume: number;
  yoy_pct: number | null;
  data_type: string;
  source_page: number | null;
}

// ── Utility Functions ──

function getTargetPeriod(): string {
  // Target is always the PREVIOUS month
  const now = new Date();
  const year = now.getMonth() === 0 ? now.getFullYear() - 1 : now.getFullYear();
  const month = now.getMonth() === 0 ? 12 : now.getMonth();
  return `${year}-${String(month).padStart(2, "0")}`;
}

function parseIndianNumeric(value: string): number | null {
  if (!value) return null;
  const clean = value.replace(/[^\d.]/g, "");
  if (!clean) return null;
  try {
    return Math.round(parseFloat(clean));
  } catch {
    return null;
  }
}

function parsePct(value: string): number | null {
  if (!value) return null;
  const match = value.match(/([+-]?\d+(?:\.\d+)?)/);
  if (match) return parseFloat(match[1]);
  return null;
}

// ── PDF Discovery ──

async function discoverPdfUrl(period: string): Promise<string | null> {
  const [year, monthStr] = period.split("-");
  const monthNum = parseInt(monthStr);
  const monthName = MONTH_NAMES[monthNum];
  const monthAbbr = monthName.substring(0, 3);

  try {
    const resp = await fetch(FADA_STATS_URL, {
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
      },
    });
    if (!resp.ok) return null;

    const html = await resp.text();
    const pdfLinks = [...html.matchAll(/href="([^"]*\.pdf[^"]*)"/gi)];

    for (const match of pdfLinks) {
      const link = match[1];
      const linkLower = link.toLowerCase();
      if (
        (linkLower.includes(monthName.toLowerCase()) || linkLower.includes(monthAbbr.toLowerCase())) &&
        link.includes(year)
      ) {
        const fullUrl = link.startsWith("http") ? link : `${FADA_BASE_URL}${link}`;
        return fullUrl;
      }
    }

    // Try common URL patterns
    const patterns = [
      `${FADA_BASE_URL}/wp-content/uploads/${year}/${monthStr}/${monthName}-${year}.pdf`,
      `${FADA_BASE_URL}/wp-content/uploads/${year}/${monthStr}/${monthAbbr}-${year}.pdf`,
    ];
    for (const url of patterns) {
      try {
        const headResp = await fetch(url, { method: "HEAD" });
        if (headResp.ok) return url;
      } catch { /* skip */ }
    }

    return null;
  } catch (e) {
    console.error("PDF discovery failed:", e);
    return null;
  }
}

// ── PDF Text Extraction ──

async function downloadAndExtractText(pdfUrl: string): Promise<{ text: string; pageTexts: string[] } | null> {
  try {
    const resp = await fetch(pdfUrl, {
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
      },
    });
    if (!resp.ok || resp.status !== 200) return null;

    const arrayBuffer = await resp.arrayBuffer();
    if (arrayBuffer.byteLength < 1000) return null;

    const pdfParse = (await import("npm:pdf-parse@1.1.1")).default;
    const buffer = new Uint8Array(arrayBuffer);
    const data = await pdfParse(buffer);

    const fullText = data.text || "";
    const pageTexts = fullText.split(/\f/).filter((p: string) => p.trim());

    return { text: fullText, pageTexts: pageTexts.length > 0 ? pageTexts : [fullText] };
  } catch (e) {
    console.error("PDF download/parse failed:", e);
    return null;
  }
}

// ── Text-based Table Parsing ──

function parseRecordsFromText(text: string, period: string): FADARecord[] {
  const records: FADARecord[] = [];
  const lines = text.split("\n").map(l => l.trim()).filter(Boolean);

  let currentSegment: string | null = null;
  let currentDataType = "retail";

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const lineLower = line.toLowerCase();

    // Detect segment headers
    for (const [key, seg] of Object.entries(FADA_SEGMENT_MAP)) {
      if (lineLower.includes(key) && (
        lineLower.includes("retail") ||
        lineLower.includes("registration") ||
        lineLower.includes("category") ||
        lineLower.includes("segment") ||
        lineLower.replace(/[^a-z\s]/g, "").trim() === key
      )) {
        currentSegment = seg;
        break;
      }
    }

    // Detect wholesale vs retail
    if (lineLower.includes("wholesale") || lineLower.includes("dispatch")) {
      currentDataType = "wholesale";
    } else if (lineLower.includes("retail") || lineLower.includes("registration")) {
      currentDataType = "retail";
    }

    // Try to parse OEM data rows
    const rowMatch = line.match(
      /^([A-Z][A-Z\s&.\-\/(),']+?)\s{2,}([\d,]+)\s+(?:([+-]?[\d.]+%?)[\s]*)?/
    );

    if (rowMatch && currentSegment) {
      const oemName = rowMatch[1].trim().toUpperCase();
      const volumeStr = rowMatch[2];
      const yoyStr = rowMatch[3] || "";

      if (NON_OEM_NAMES.has(oemName)) continue;
      if (oemName.length < 3) continue;

      const volume = parseIndianNumeric(volumeStr);
      const yoyPct = parsePct(yoyStr);

      if (volume && volume > 0) {
        records.push({
          report_period: period,
          oem_name: oemName,
          segment: currentSegment,
          volume,
          yoy_pct: yoyPct,
          data_type: currentDataType === "wholesale" ? "wholesale" : "actual",
          source_page: null,
        });
      }
    }

    // Tab-separated format
    const tabMatch = line.match(
      /^([A-Z][A-Z\s&.\-\/(),']+?)\t+([\d,]+)\t+(?:([+-]?[\d.]+%?))?/
    );
    if (tabMatch && currentSegment && !rowMatch) {
      const oemName = tabMatch[1].trim().toUpperCase();
      const volumeStr = tabMatch[2];
      const yoyStr = tabMatch[3] || "";

      if (NON_OEM_NAMES.has(oemName)) continue;
      if (oemName.length < 3) continue;

      const volume = parseIndianNumeric(volumeStr);
      const yoyPct = parsePct(yoyStr);

      if (volume && volume > 0) {
        records.push({
          report_period: period,
          oem_name: oemName,
          segment: currentSegment,
          volume,
          yoy_pct: yoyPct,
          data_type: "actual",
          source_page: null,
        });
      }
    }
  }

  // Deduplicate
  const seen = new Set<string>();
  return records.filter(r => {
    const key = `${r.oem_name}|${r.segment}|${r.data_type}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

// ── Validation ──

interface ValidationResult {
  valid: boolean;
  errors: string[];
  warnings: string[];
}

function validateRecords(records: FADARecord[], period: string): ValidationResult {
  const errors: string[] = [];
  const warnings: string[] = [];

  const segments = new Set(records.map(r => r.segment));
  const mainSegments = ["PV", "CV", "2W"].filter(s => segments.has(s));
  if (mainSegments.length < 2) {
    errors.push(`Only ${mainSegments.length} main segments found: ${mainSegments.join(", ")}. Expected at least 2 of PV/CV/2W.`);
  }

  for (const seg of mainSegments) {
    const segRecords = records.filter(r => r.segment === seg && r.data_type === "actual");
    if (segRecords.length < 5) {
      warnings.push(`${seg}: Only ${segRecords.length} OEMs found (expected 5+).`);
    }
  }

  const pvTotal = records
    .filter(r => r.segment === "PV" && r.data_type === "actual")
    .reduce((sum, r) => sum + r.volume, 0);
  if (pvTotal > 0 && (pvTotal < 100_000 || pvTotal > 800_000)) {
    errors.push(`PV total ${pvTotal.toLocaleString()} is outside expected range (100k-800k).`);
  }

  const cvTotal = records
    .filter(r => r.segment === "CV" && r.data_type === "actual")
    .reduce((sum, r) => sum + r.volume, 0);
  if (cvTotal > 0 && (cvTotal < 30_000 || cvTotal > 250_000)) {
    warnings.push(`CV total ${cvTotal.toLocaleString()} is outside typical range (30k-250k).`);
  }

  const twTotal = records
    .filter(r => r.segment === "2W" && r.data_type === "actual")
    .reduce((sum, r) => sum + r.volume, 0);
  if (twTotal > 0 && (twTotal < 500_000 || twTotal > 3_000_000)) {
    warnings.push(`2W total ${twTotal.toLocaleString()} is outside typical range (500k-3M).`);
  }

  for (const seg of mainSegments) {
    const segRecords = records.filter(r => r.segment === seg && r.data_type === "actual");
    const segTotal = segRecords.reduce((sum, r) => sum + r.volume, 0);
    for (const r of segRecords) {
      if (segTotal > 0 && r.volume > segTotal * 0.5) {
        warnings.push(`${seg}: ${r.oem_name} has ${((r.volume / segTotal) * 100).toFixed(1)}% share — unusually high.`);
      }
    }
  }

  return { valid: errors.length === 0, errors, warnings };
}

// ── Main Handler ──

Deno.serve(async (req: Request) => {
  const startTime = Date.now();

  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
    const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
    const supabase = createClient(supabaseUrl, supabaseServiceKey);

    const targetPeriod = getTargetPeriod();
    const today = new Date();
    const dayOfMonth = today.getDate();

    console.log(`FADA extraction: target=${targetPeriod}, day=${dayOfMonth}`);

    const body = req.method === "POST" ? await req.json().catch(() => ({})) : {};
    const isForced = body.force === true;

    if (!isForced && (dayOfMonth < 7 || dayOfMonth > 11)) {
      return new Response(JSON.stringify({
        status: "skipped",
        reason: `Day ${dayOfMonth} is outside 7-11 window`,
        target_period: targetPeriod,
      }), { headers: { "Content-Type": "application/json" } });
    }

    // Log run start
    const { data: logEntry } = await supabase
      .from("cron_run_log")
      .insert({
        job_name: "fada-monthly-extract",
        target_period: targetPeriod,
        status: "started",
        metadata: { day_of_month: dayOfMonth, forced: isForced },
      })
      .select("id")
      .single();
    const logId = logEntry?.id;

    // Check if data already exists
    const { count: existingCount } = await supabase
      .from("raw_fada_report")
      .select("*", { count: "exact", head: true })
      .eq("report_period", targetPeriod)
      .eq("data_type", "actual")
      .not("oem_name", "in", "(INDUSTRY_TOTAL)");

    if (existingCount && existingCount > 10 && !isForced) {
      if (logId) {
        await supabase.from("cron_run_log").update({
          status: "skipped",
          completed_at: new Date().toISOString(),
          metadata: { reason: `${existingCount} records already exist`, day_of_month: dayOfMonth },
        }).eq("id", logId);
      }

      return new Response(JSON.stringify({
        status: "skipped",
        reason: `${existingCount} records already exist for ${targetPeriod}`,
        target_period: targetPeriod,
      }), { headers: { "Content-Type": "application/json" } });
    }

    // Step 1: Discover PDF URL
    console.log("Discovering FADA PDF URL...");
    const pdfUrl = await discoverPdfUrl(targetPeriod);

    if (!pdfUrl) {
      if (logId) {
        await supabase.from("cron_run_log").update({
          status: "failed",
          error_message: "PDF not found on FADA website",
          completed_at: new Date().toISOString(),
        }).eq("id", logId);
      }
      return new Response(JSON.stringify({
        status: "failed",
        error: `FADA PDF not found for ${targetPeriod}`,
        target_period: targetPeriod,
      }), { headers: { "Content-Type": "application/json" }, status: 404 });
    }

    console.log(`Found PDF: ${pdfUrl}`);

    // Step 2: Download and extract text
    const extracted = await downloadAndExtractText(pdfUrl);
    if (!extracted) {
      if (logId) {
        await supabase.from("cron_run_log").update({
          status: "failed",
          error_message: "Failed to download or parse PDF",
          completed_at: new Date().toISOString(),
          metadata: { pdf_url: pdfUrl },
        }).eq("id", logId);
      }
      return new Response(JSON.stringify({
        status: "failed",
        error: "Failed to download or parse PDF",
        pdf_url: pdfUrl,
      }), { headers: { "Content-Type": "application/json" }, status: 500 });
    }

    // Step 3: Parse records
    const records = parseRecordsFromText(extracted.text, targetPeriod);
    console.log(`Parsed ${records.length} records`);

    // Step 4: Validate
    const validation = validateRecords(records, targetPeriod);
    console.log("Validation:", JSON.stringify(validation));

    if (!validation.valid) {
      if (logId) {
        await supabase.from("cron_run_log").update({
          status: "failed",
          error_message: `Validation failed: ${validation.errors.join("; ")}`,
          completed_at: new Date().toISOString(),
          metadata: { pdf_url: pdfUrl, records_parsed: records.length, validation },
        }).eq("id", logId);
      }
      return new Response(JSON.stringify({
        status: "failed",
        error: "Validation failed",
        validation,
        records_parsed: records.length,
      }), { headers: { "Content-Type": "application/json" }, status: 422 });
    }

    // Step 5: Upsert records (only 'actual', never modify historical)
    let insertedCount = 0;
    const segmentsInserted = new Set<string>();

    for (const record of records) {
      if (record.data_type !== "actual") continue;

      const { error } = await supabase
        .from("raw_fada_report")
        .upsert(
          {
            report_period: record.report_period,
            oem_name: record.oem_name,
            segment: record.segment,
            volume: record.volume,
            yoy_pct: record.yoy_pct,
            data_type: "actual",
            source_page: record.source_page,
            extracted_at: new Date().toISOString(),
          },
          { onConflict: "report_period,oem_name,segment,data_type" }
        );

      if (!error) {
        insertedCount++;
        segmentsInserted.add(record.segment);
      } else {
        console.error(`Upsert failed for ${record.oem_name}/${record.segment}:`, error.message);
      }
    }

    // Step 6: Log success
    const duration = Date.now() - startTime;
    if (logId) {
      await supabase.from("cron_run_log").update({
        status: "success",
        records_inserted: insertedCount,
        segments_found: Array.from(segmentsInserted),
        completed_at: new Date().toISOString(),
        metadata: {
          pdf_url: pdfUrl,
          records_parsed: records.length,
          validation,
          duration_ms: duration,
        },
      }).eq("id", logId);
    }

    return new Response(JSON.stringify({
      status: "success",
      target_period: targetPeriod,
      records_inserted: insertedCount,
      segments: Array.from(segmentsInserted),
      validation,
      pdf_url: pdfUrl,
      duration_ms: duration,
    }), {
      headers: { "Content-Type": "application/json" },
    });

  } catch (e) {
    console.error("FADA extraction error:", e);
    return new Response(JSON.stringify({
      status: "error",
      error: String(e),
    }), {
      headers: { "Content-Type": "application/json" },
      status: 500,
    });
  }
});
