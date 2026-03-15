/**
 * AutoQuant — Database Query Functions.
 *
 * All dashboard data fetching goes through these functions.
 * They query the materialized view and fact tables via Supabase.
 */

import { supabase } from "./supabase";
import type {
  IndustryPulseData,
  SegmentShare,
  DailyDataPoint,
  OEMDeepDiveData,
  OEM,
  ScorecardRow,
  RevenueEstimate,
  HistoricalData,
  HistoricalYearSummary,
  HistoricalMonthPoint,
  FADADashboardData,
  FADAMonthData,
  FADAOemRow,
  FADAFYData,
  FADAFYOemRow,
} from "./types";

// ── Industry Pulse Queries ──

export async function fetchIndustryPulse(): Promise<IndustryPulseData> {
  const now = new Date();
  const currentMonth = now.getMonth() + 1;
  const currentYear = now.getFullYear();

  // Latest date with data
  const { data: latestRow } = await supabase
    .from("fact_daily_registrations")
    .select("data_date")
    .order("data_date", { ascending: false })
    .limit(1)
    .single();

  const latestDate = latestRow?.data_date || now.toISOString().split("T")[0];

  // MTD totals by segment from materialized view
  const { data: mtdData } = await supabase
    .from("mv_oem_monthly_summary")
    .select("segment_code, total_registrations")
    .eq("calendar_year", currentYear)
    .eq("calendar_month", currentMonth);

  const segmentTotals: Record<string, number> = { PV: 0, CV: 0, "2W": 0 };
  let mtdTotal = 0;

  (mtdData || []).forEach((row: any) => {
    const seg = row.segment_code;
    const vol = Number(row.total_registrations) || 0;
    if (seg in segmentTotals) {
      segmentTotals[seg] += vol;
      mtdTotal += vol;
    }
  });

  // Segment breakdown for donut
  const segmentBreakdown: SegmentShare[] = Object.entries(segmentTotals).map(
    ([segment, value]) => ({
      segment,
      value,
      percentage: mtdTotal > 0 ? (value / mtdTotal) * 100 : 0,
    })
  );

  // Daily series (last 60 days) from fact table
  const sixtyDaysAgo = new Date(now.getTime() - 60 * 24 * 60 * 60 * 1000)
    .toISOString()
    .split("T")[0];

  const { data: dailyRaw } = await supabase.rpc("get_daily_series", {
    start_date: sixtyDaysAgo,
  });

  // Build daily series with 7-day MA
  const dailySeries: DailyDataPoint[] = buildDailySeries(dailyRaw || []);

  // YoY calculations need prior year data
  const priorYear = currentYear - 1;
  const { data: priorMtd } = await supabase
    .from("mv_oem_monthly_summary")
    .select("total_registrations")
    .eq("calendar_year", priorYear)
    .eq("calendar_month", currentMonth);

  const priorMtdTotal = (priorMtd || []).reduce(
    (sum: number, r: any) => sum + (Number(r.total_registrations) || 0),
    0
  );
  const mtdYoY = priorMtdTotal > 0 ? ((mtdTotal - priorMtdTotal) / priorMtdTotal) * 100 : 0;

  return {
    latestDate,
    latestDayTotal: dailySeries.length > 0 ? dailySeries[dailySeries.length - 1].total : 0,
    mtdTotal,
    qtdTotal: mtdTotal, // Simplified — full QTD needs quarter aggregation
    ytdTotal: mtdTotal, // Simplified — full YTD needs year aggregation
    mtdYoY,
    qtdYoY: mtdYoY, // Placeholder
    ytdYoY: mtdYoY, // Placeholder
    segmentBreakdown,
    dailySeries,
    evPenetration: [],
    fuelMixTrend: [],
    dataFreshness: latestDate,
  };
}

function buildDailySeries(raw: any[]): DailyDataPoint[] {
  // Group by date
  const byDate: Record<string, { pv: number; cv: number; tw: number }> = {};

  for (const row of raw) {
    const d = row.data_date;
    if (!byDate[d]) byDate[d] = { pv: 0, cv: 0, tw: 0 };
    const seg = row.segment_code;
    const vol = Number(row.registrations) || 0;
    if (seg === "PV") byDate[d].pv += vol;
    else if (seg === "CV") byDate[d].cv += vol;
    else if (seg === "2W") byDate[d].tw += vol;
  }

  const sorted = Object.entries(byDate)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([date, vals]) => ({
      date,
      ...vals,
      total: vals.pv + vals.cv + vals.tw,
      ma7: 0,
    }));

  // Compute 7-day moving average
  for (let i = 0; i < sorted.length; i++) {
    const window = sorted.slice(Math.max(0, i - 6), i + 1);
    sorted[i].ma7 = Math.round(
      window.reduce((s, r) => s + r.total, 0) / window.length
    );
  }

  return sorted;
}

// ── OEM Deep Dive Queries ──

export async function fetchOEMList(): Promise<OEM[]> {
  const { data } = await supabase
    .from("dim_oem")
    .select("*")
    .eq("is_listed", true)
    .eq("is_in_scope", true)
    .order("oem_name");

  return (data || []) as OEM[];
}

export async function fetchOEMDeepDive(
  ticker: string
): Promise<OEMDeepDiveData | null> {
  // Get OEM details
  const { data: oem } = await supabase
    .from("dim_oem")
    .select("*")
    .eq("nse_ticker", ticker)
    .single();

  if (!oem) return null;

  const now = new Date();
  const currentMonth = now.getMonth() + 1;
  const currentYear = now.getFullYear();

  // MTD volume
  const { data: mtdRows } = await supabase
    .from("mv_oem_monthly_summary")
    .select("total_registrations, powertrain, segment_code")
    .eq("nse_ticker", ticker)
    .eq("calendar_year", currentYear)
    .eq("calendar_month", currentMonth);

  const mtdVolume = (mtdRows || []).reduce(
    (sum: number, r: any) => sum + (Number(r.total_registrations) || 0),
    0
  );

  // Monthly ICE vs EV split (last 12 months)
  const { data: monthlyData } = await supabase
    .from("mv_oem_monthly_summary")
    .select("calendar_year, calendar_month, powertrain, total_registrations")
    .eq("nse_ticker", ticker)
    .gte("calendar_year", currentYear - 1)
    .order("calendar_year")
    .order("calendar_month");

  const iceEvSplit = buildICEvsEV(monthlyData || []);

  return {
    oem: oem as OEM,
    mtdVolume,
    qtdVolume: mtdVolume,
    ytdVolume: mtdVolume,
    mtdYoY: 0,
    qtdYoY: 0,
    ytdYoY: 0,
    mtdMoM: 0,
    marketShareTrend: [],
    iceEvSplit,
    segmentBreakdown: [],
  };
}

function buildICEvsEV(data: any[]): { month: string; ice: number; ev: number }[] {
  const byMonth: Record<string, { ice: number; ev: number }> = {};

  for (const row of data) {
    const key = `${row.calendar_year}-${String(row.calendar_month).padStart(2, "0")}`;
    if (!byMonth[key]) byMonth[key] = { ice: 0, ev: 0 };
    const vol = Number(row.total_registrations) || 0;
    if (row.powertrain === "EV") byMonth[key].ev += vol;
    else byMonth[key].ice += vol;
  }

  return Object.entries(byMonth)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([month, vals]) => ({ month, ...vals }));
}

// ── Scorecard Query ──

export async function fetchScorecard(): Promise<ScorecardRow[]> {
  const now = new Date();
  const currentMonth = now.getMonth() + 1;
  const currentYear = now.getFullYear();

  const { data } = await supabase
    .from("mv_oem_monthly_summary")
    .select("oem_name, nse_ticker, is_listed, segment_code, powertrain, total_registrations")
    .eq("calendar_year", currentYear)
    .eq("calendar_month", currentMonth)
    .eq("is_listed", true);

  // Aggregate by OEM
  const byOEM: Record<string, ScorecardRow> = {};

  for (const row of data || []) {
    const key = row.nse_ticker || row.oem_name;
    if (!byOEM[key]) {
      byOEM[key] = {
        oem_name: row.oem_name,
        nse_ticker: row.nse_ticker || "",
        segment: row.segment_code,
        qtd_volume: 0,
        yoy_pct: 0,
        market_share_pct: 0,
        ev_pct: 0,
        est_rev_cr: null,
        confidence: "MEDIUM",
      };
    }
    byOEM[key].qtd_volume += Number(row.total_registrations) || 0;
  }

  return Object.values(byOEM).sort((a, b) => b.qtd_volume - a.qtd_volume);
}

// ── Revenue Estimator Queries ──

/**
 * Fetch demand-based revenue proxy estimates.
 *
 * Formula: est_domestic_rev_cr = registrations × segment_ASP / 1e7
 * ASP assumptions from fact_asp_master.
 *
 * DISCLAIMER: This is NOT accounting revenue. It is a demand-based proxy
 * using registrations × assumed ASPs.
 */
export async function fetchRevenueEstimates(): Promise<RevenueEstimate[]> {
  const now = new Date();
  const currentMonth = now.getMonth() + 1;
  const currentYear = now.getFullYear();

  // Determine current FY quarter
  // Indian FY: Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar
  const fyYear = currentMonth <= 3 ? currentYear - 1 : currentYear;
  const fyQuarterNum =
    currentMonth >= 4 && currentMonth <= 6
      ? 1
      : currentMonth >= 7 && currentMonth <= 9
        ? 2
        : currentMonth >= 10 && currentMonth <= 12
          ? 3
          : 4;
  const fyQuarter = `FY${fyYear}-${(fyYear + 1) % 100}Q${fyQuarterNum}`;

  // Get ASP assumptions
  const { data: aspData } = await supabase
    .from("fact_asp_master")
    .select("*")
    .eq("fy_year", `FY${fyYear}-${String((fyYear + 1) % 100).padStart(2, "0")}`);

  const aspMap: Record<string, { asp_lakhs: number; low: number; high: number }> = {};
  for (const row of aspData || []) {
    const key = `${row.segment_code}_${row.powertrain}`;
    aspMap[key] = {
      asp_lakhs: Number(row.asp_inr_lakhs) || 0,
      low: Number(row.asp_low_inr_lakhs) || 0,
      high: Number(row.asp_high_inr_lakhs) || 0,
    };
  }

  // Get current quarter OEM registrations by segment + powertrain
  const quarterMonths = getQuarterMonths(fyQuarterNum);
  const { data: regData } = await supabase
    .from("mv_oem_monthly_summary")
    .select(
      "oem_name, nse_ticker, is_listed, segment_code, powertrain, total_registrations, calendar_month"
    )
    .eq("calendar_year", currentYear)
    .eq("is_listed", true)
    .in("calendar_month", quarterMonths);

  // Aggregate by OEM and compute revenue
  const byOEM: Record<
    string,
    {
      oem_name: string;
      nse_ticker: string;
      reg_volume: number;
      rev_cr: number;
      rev_low_cr: number;
      rev_high_cr: number;
      months_with_data: Set<number>;
    }
  > = {};

  for (const row of regData || []) {
    const key = row.nse_ticker || row.oem_name;
    if (!byOEM[key]) {
      byOEM[key] = {
        oem_name: row.oem_name,
        nse_ticker: row.nse_ticker || "",
        reg_volume: 0,
        rev_cr: 0,
        rev_low_cr: 0,
        rev_high_cr: 0,
        months_with_data: new Set(),
      };
    }

    const vol = Number(row.total_registrations) || 0;
    byOEM[key].reg_volume += vol;
    byOEM[key].months_with_data.add(row.calendar_month);

    // Compute revenue proxy using ASP
    const aspKey = `${row.segment_code}_${row.powertrain}`;
    const asp = aspMap[aspKey];
    if (asp && asp.asp_lakhs > 0) {
      // Revenue in Cr = volume × ASP_lakhs / 100
      byOEM[key].rev_cr += (vol * asp.asp_lakhs) / 100;
      byOEM[key].rev_low_cr += (vol * asp.low) / 100;
      byOEM[key].rev_high_cr += (vol * asp.high) / 100;
    }
  }

  const totalQuarterMonths = quarterMonths.length;

  return Object.values(byOEM)
    .map((oem) => ({
      oem_name: oem.oem_name,
      nse_ticker: oem.nse_ticker,
      fy_quarter: fyQuarter,
      reg_volume: oem.reg_volume,
      est_domestic_rev_cr: Math.round(oem.rev_cr),
      est_rev_low_cr: Math.round(oem.rev_low_cr),
      est_rev_high_cr: Math.round(oem.rev_high_cr),
      data_completeness_pct:
        totalQuarterMonths > 0
          ? Math.round((oem.months_with_data.size / totalQuarterMonths) * 100)
          : 0,
    }))
    .sort((a, b) => b.est_domestic_rev_cr - a.est_domestic_rev_cr);
}

function getQuarterMonths(quarterNum: number): number[] {
  switch (quarterNum) {
    case 1:
      return [4, 5, 6];
    case 2:
      return [7, 8, 9];
    case 3:
      return [10, 11, 12];
    case 4:
      return [1, 2, 3];
    default:
      return [1, 2, 3];
  }
}

// ── Historical Data Queries ──

/**
 * Fetch historical data spanning 2016-present.
 *
 * Aggregates from mv_oem_monthly_summary to build:
 *   - Year-level summaries with confidence indicators
 *   - Monthly time series for trend charts
 *   - Data coverage metrics
 */
export async function fetchHistoricalData(): Promise<HistoricalData> {
  // Fetch all monthly data from MV
  const { data: mvData } = await supabase
    .from("mv_oem_monthly_summary")
    .select(
      "calendar_year, calendar_month, segment_code, powertrain, total_registrations, oem_id"
    )
    .gte("calendar_year", 2016)
    .order("calendar_year")
    .order("calendar_month");

  const rows = mvData || [];

  // ── Build year summaries ──
  const yearMap: Record<
    number,
    {
      total: number;
      pv: number;
      cv: number;
      tw: number;
      ev: number;
      oems: Set<number>;
      months: Set<number>;
      sources: Set<string>;
    }
  > = {};

  for (const row of rows) {
    const yr = row.calendar_year;
    if (!yearMap[yr]) {
      yearMap[yr] = {
        total: 0,
        pv: 0,
        cv: 0,
        tw: 0,
        ev: 0,
        oems: new Set(),
        months: new Set(),
        sources: new Set(),
      };
    }
    const vol = Number(row.total_registrations) || 0;
    yearMap[yr].total += vol;
    yearMap[yr].oems.add(row.oem_id);
    yearMap[yr].months.add(row.calendar_month);

    if (row.segment_code === "PV") yearMap[yr].pv += vol;
    else if (row.segment_code === "CV") yearMap[yr].cv += vol;
    else if (row.segment_code === "2W") yearMap[yr].tw += vol;

    if (row.powertrain === "EV") yearMap[yr].ev += vol;
  }

  const sortedYears = Object.keys(yearMap)
    .map(Number)
    .sort();

  const yearSummaries: HistoricalYearSummary[] = sortedYears.map(
    (year, idx) => {
      const y = yearMap[year];
      const prevYear = idx > 0 ? yearMap[sortedYears[idx - 1]] : null;
      const yoy =
        prevYear && prevYear.total > 0
          ? ((y.total - prevYear.total) / prevYear.total) * 100
          : null;

      // Confidence based on months of data and source
      const monthsCovered = y.months.size;
      let confidence: "HIGH" | "MEDIUM" | "LOW";
      if (year >= 2025) confidence = "HIGH";     // Live VAHAN data
      else if (monthsCovered >= 10) confidence = "MEDIUM"; // Historical with good coverage
      else confidence = "LOW";                   // Sparse historical

      // FY label: calendar year 2016 contributes to FY16 (Jan-Mar) and FY17 (Apr-Dec)
      // Simplify: show the dominant FY
      const fyYear = year + 1;
      const fyLabel = `FY${fyYear % 100 === 0 ? "00" : String(fyYear % 100).padStart(2, "0")}`;

      // Data source inference
      let dataSource = "SIAM_HISTORICAL";
      if (year >= 2025) dataSource = "VAHAN_DAILY";
      else if (year >= 2024) dataSource = "VAHAN_DAILY + SIAM";

      return {
        year,
        fy_label: fyLabel,
        total_registrations: y.total,
        pv_registrations: y.pv,
        cv_registrations: y.cv,
        tw_registrations: y.tw,
        ev_registrations: y.ev,
        ev_pct: y.total > 0 ? (y.ev / y.total) * 100 : 0,
        yoy_pct: yoy !== null ? Math.round(yoy * 10) / 10 : null,
        oems_with_data: y.oems.size,
        data_source: dataSource,
        confidence,
        months_with_data: monthsCovered,
      };
    }
  );

  // ── Build monthly trend ──
  const monthMap: Record<
    string,
    { pv: number; cv: number; tw: number; total: number; ev: number }
  > = {};

  for (const row of rows) {
    const key = `${row.calendar_year}-${String(row.calendar_month).padStart(2, "0")}`;
    if (!monthMap[key]) {
      monthMap[key] = { pv: 0, cv: 0, tw: 0, total: 0, ev: 0 };
    }
    const vol = Number(row.total_registrations) || 0;
    monthMap[key].total += vol;

    if (row.segment_code === "PV") monthMap[key].pv += vol;
    else if (row.segment_code === "CV") monthMap[key].cv += vol;
    else if (row.segment_code === "2W") monthMap[key].tw += vol;

    if (row.powertrain === "EV") monthMap[key].ev += vol;
  }

  const monthlyTrend: HistoricalMonthPoint[] = Object.entries(monthMap)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([month, vals]) => ({
      month,
      pv: vals.pv,
      cv: vals.cv,
      tw: vals.tw,
      total: vals.total,
      ev_total: vals.ev,
      source: parseInt(month) >= 2025 ? "VAHAN" : "SIAM",
    }));

  // ── Coverage metrics ──
  const allMonths = monthlyTrend.map((m) => m.month);
  const minMonth = allMonths.length > 0 ? allMonths[0] : "";
  const maxMonth = allMonths.length > 0 ? allMonths[allMonths.length - 1] : "";

  // Expected months from Jan 2016 to now
  const now = new Date();
  const expectedMonths =
    (now.getFullYear() - 2016) * 12 + now.getMonth() + 1;
  const coveragePct =
    expectedMonths > 0
      ? Math.min(100, (allMonths.length / expectedMonths) * 100)
      : 0;

  return {
    yearSummaries,
    monthlyTrend,
    dataRange: { min: minMonth, max: maxMonth },
    totalRecords: rows.length,
    coveragePct: Math.round(coveragePct * 10) / 10,
  };
}

// ── FADA Retail Data Queries ──

/** Non-OEM entries to filter out (fuel type aggregates) */
const NON_OEM_NAMES = new Set([
  "DIESEL", "EV", "PETROL/ETHANOL", "CNG/LPG", "HYBRID",
  "METHANOL", "OTHERS INCLUDING EV",
]);

/**
 * OEM Group Consolidation Maps.
 *
 * FADA data contains both group-level totals (e.g. "SKODA AUTO VOLKSWAGEN GROUP")
 * and subsidiary-level rows (e.g. "SKODA AUTO VOLKSWAGEN INDIA PVT LTD", "AUDI AG").
 * The subsidiary volumes are ALREADY included in the group total, so we must:
 *   1. Keep the group row as the main entry (its volume IS the consolidated total)
 *   2. Attach subsidiary rows as children for drill-down
 *   3. Remove subsidiary rows from the top-level list to avoid double-counting
 *
 * When only subsidiaries exist (no group row for that period), we sum them into
 * a synthetic group entry.
 */

/** Maps a group display name → array of subsidiary OEM names */
const OEM_GROUP_MAP: Record<string, { groupNames: string[]; subsidiaries: string[] }> = {
  "SKODA AUTO VOLKSWAGEN GROUP": {
    groupNames: ["SKODA AUTO VOLKSWAGEN GROUP"],
    subsidiaries: [
      "SKODA AUTO VOLKSWAGEN INDIA PVT LTD",
      "AUDI AG",
      "VOLKSWAGEN AG/INDIA PVT. LTD.",
      "PORSCHE AG GERMANY",
      "AUTOMOBILI LAMBORGHINI S.P.A",
      "SKODA AUTO INDIA/AS PVT LTD",
    ],
  },
  "MERCEDES-BENZ GROUP": {
    groupNames: ["MERCEDES -BENZ GROUP", "MERCEDES BENZ"],
    subsidiaries: [
      "MERCEDES-BENZ INDIA PVT LTD",
      "MERCEDES -BENZ AG",
      "DAIMLER AG",
    ],
  },
  "STELLANTIS GROUP": {
    groupNames: ["STELLANTIS GROUP"],
    subsidiaries: [
      "STELLANTIS AUTOMOBILES INDIA PVT LTD",
      "STELLANTIS INDIA PVT LTD",
      "PCA AUTOMOBILES INDIA PVT LTD",
      "PCA AUTOMOBILI INDIA PVT LTD",
      "FIAT INDIA AUTOMOBILES PVT LTD",
    ],
  },
  "BAJAJ AUTO GROUP": {
    groupNames: ["BAJAJ AUTO GROUP"],
    subsidiaries: [
      "BAJAJ AUTO LTD",
      "BAJAJ AUTO",
      "CHETAK TECHNOLOGY LIMITED",
    ],
  },
  "MAHINDRA GROUP": {
    groupNames: [], // No dedicated group row; M&M acts as group
    subsidiaries: [
      "MAHINDRA LAST MILE MOBILITY LTD",
    ],
  },
};

/** Reverse lookup: subsidiary name → canonical group name */
const SUBSIDIARY_TO_GROUP: Record<string, string> = {};
/** Group-level OEM names (the row that already contains the consolidated total) */
const GROUP_ROW_NAMES = new Set<string>();

for (const [canonicalGroup, config] of Object.entries(OEM_GROUP_MAP)) {
  for (const sub of config.subsidiaries) {
    SUBSIDIARY_TO_GROUP[sub] = canonicalGroup;
  }
  for (const gn of config.groupNames) {
    SUBSIDIARY_TO_GROUP[gn] = canonicalGroup;
    GROUP_ROW_NAMES.add(gn);
  }
}

/** Special case: MAHINDRA & MAHINDRA LIMITED acts as the group parent for CV segment */
const MAHINDRA_PARENT = "MAHINDRA & MAHINDRA LIMITED";

/**
 * Consolidates OEM rows into groups.
 * - Group rows keep their volume (which already includes subsidiaries).
 * - Subsidiary rows become children of the group.
 * - If no group row exists for a period but subsidiaries do, we sum them.
 */
function consolidateOEMRows<T extends { oem_name: string; volume: number }>(
  oems: T[],
  segment: string,
): T[] {
  // Identify which OEMs belong to groups
  const groupBuckets: Record<string, { groupRow: T | null; children: T[] }> = {};
  const standalone: T[] = [];

  for (const oem of oems) {
    // Special Mahindra logic: only consolidate in CV segment
    if (segment === "CV" && oem.oem_name === "MAHINDRA LAST MILE MOBILITY LTD") {
      const groupName = "MAHINDRA GROUP";
      if (!groupBuckets[groupName]) groupBuckets[groupName] = { groupRow: null, children: [] };
      groupBuckets[groupName].children.push(oem);
      continue;
    }
    if (segment === "CV" && oem.oem_name === MAHINDRA_PARENT) {
      const groupName = "MAHINDRA GROUP";
      if (!groupBuckets[groupName]) groupBuckets[groupName] = { groupRow: null, children: [] };
      // M&M is the parent — treat it as both group row and a child for display
      groupBuckets[groupName].groupRow = oem;
      groupBuckets[groupName].children.push(oem);
      continue;
    }

    const canonicalGroup = SUBSIDIARY_TO_GROUP[oem.oem_name];
    if (canonicalGroup) {
      if (!groupBuckets[canonicalGroup]) groupBuckets[canonicalGroup] = { groupRow: null, children: [] };
      if (GROUP_ROW_NAMES.has(oem.oem_name)) {
        groupBuckets[canonicalGroup].groupRow = oem;
      } else {
        groupBuckets[canonicalGroup].children.push(oem);
      }
    } else {
      standalone.push(oem);
    }
  }

  // Build consolidated list
  const result: T[] = [...standalone];

  for (const [canonicalGroup, bucket] of Object.entries(groupBuckets)) {
    if (bucket.groupRow) {
      // Group row exists — use its volume (already consolidated), attach children
      const groupEntry = {
        ...bucket.groupRow,
        oem_name: canonicalGroup,
        is_group: true,
        children: bucket.children.length > 0
          ? bucket.children.sort((a, b) => b.volume - a.volume)
          : undefined,
      };
      result.push(groupEntry);
    } else if (bucket.children.length > 0) {
      // No group row — sum subsidiaries into a synthetic group
      const totalVol = bucket.children.reduce((sum, c) => sum + c.volume, 0);
      const syntheticGroup = {
        ...bucket.children[0], // Copy shape from first child
        oem_name: canonicalGroup,
        volume: totalVol,
        is_group: true,
        children: bucket.children.sort((a, b) => b.volume - a.volume),
      };
      result.push(syntheticGroup);
    }
  }

  return result;
}

/** Convert YYYY-MM period to Indian Financial Year label */
function periodToFY(period: string): string {
  const [y, m] = period.split("-").map(Number);
  const fy = m >= 4 ? y + 1 : y;
  return `FY${String(fy % 100).padStart(2, "0")}`;
}

/** Compute prior month period: "2025-04" → "2025-03", "2025-01" → "2024-12" */
function priorMonth(period: string): string {
  const [y, m] = period.split("-").map(Number);
  if (m === 1) return `${y - 1}-12`;
  return `${y}-${String(m - 1).padStart(2, "0")}`;
}

/**
 * Fetch FADA monthly OEM retail data across all segments.
 * Returns monthly + FY grouped data with OEM-level detail,
 * market share, YoY growth, and MoM growth.
 */
export async function fetchFADAData(): Promise<FADADashboardData> {
  // Supabase PostgREST max-rows is 1000 by default; we have ~2900 rows.
  // Paginate with five date-range queries to keep each batch under 1000.
  const selectCols = "report_period, oem_name, segment, volume, yoy_pct";
  const baseQuery = () =>
    supabase
      .from("raw_fada_report")
      .select(selectCols)
      .eq("data_type", "actual")
      .order("report_period", { ascending: false })
      .order("segment")
      .order("volume", { ascending: false });

  const [res1, res2, res3, res4, res5] = await Promise.all([
    baseQuery().gte("report_period", "2025-06"),                                    // ~430 rows
    baseQuery().gte("report_period", "2024-06").lt("report_period", "2025-06"),     // ~550 rows
    baseQuery().gte("report_period", "2023-06").lt("report_period", "2024-06"),     // ~540 rows
    baseQuery().gte("report_period", "2022-06").lt("report_period", "2023-06"),     // ~660 rows
    baseQuery().lt("report_period", "2022-06"),                                     // ~700 rows
  ]);

  for (const [i, res] of [res1, res2, res3, res4, res5].entries()) {
    if (res.error) {
      console.error(`[FADA] Query error (batch${i + 1}):`, res.error);
      throw new Error("Failed to fetch FADA data");
    }
  }

  const rawRows = [
    ...(res1.data || []),
    ...(res2.data || []),
    ...(res3.data || []),
    ...(res4.data || []),
    ...(res5.data || []),
  ];

  const rows = rawRows.filter(
    (r: any) => !NON_OEM_NAMES.has(r.oem_name)
  );

  // ── Build monthly groups ──
  const groupKey = (r: any) => `${r.report_period}__${r.segment}`;
  const groups: Record<
    string,
    { period: string; segment: string; oems: FADAOemRow[]; total: number }
  > = {};

  for (const row of rows) {
    const key = groupKey(row);
    if (!groups[key]) {
      groups[key] = { period: row.report_period, segment: row.segment, oems: [], total: 0 };
    }
    const vol = Number(row.volume) || 0;
    groups[key].total += vol;
    groups[key].oems.push({
      oem_name: row.oem_name,
      volume: vol,
      market_share_pct: 0,
      yoy_pct: row.yoy_pct != null ? Number(row.yoy_pct) : null,
      mom_pct: null,
    });
  }

  // Consolidate OEM groups and recompute totals + market share
  for (const g of Object.values(groups)) {
    const consolidated = consolidateOEMRows(g.oems, g.segment);
    // Recalculate total from consolidated (non-double-counted) rows
    g.total = consolidated.reduce((sum, o) => sum + o.volume, 0);
    g.oems = consolidated;
    for (const oem of g.oems) {
      oem.market_share_pct = g.total > 0 ? Math.round((oem.volume / g.total) * 1000) / 10 : 0;
      // Recompute market share for children too
      if (oem.children) {
        for (const child of oem.children) {
          child.market_share_pct = g.total > 0 ? Math.round((child.volume / g.total) * 1000) / 10 : 0;
        }
      }
    }
  }

  // Build OEM volume lookup: period__segment__oem -> volume
  const volLookup: Record<string, number> = {};
  const totalLookup: Record<string, number> = {};
  for (const g of Object.values(groups)) {
    totalLookup[`${g.period}__${g.segment}`] = g.total;
    for (const oem of g.oems) {
      volLookup[`${g.period}__${g.segment}__${oem.oem_name}`] = oem.volume;
    }
  }

  // Compute YoY and MoM
  for (const g of Object.values(groups)) {
    const [yearStr, monthStr] = g.period.split("-");
    const priorYearPeriod = `${Number(yearStr) - 1}-${monthStr}`;
    const priorMo = priorMonth(g.period);

    for (const oem of g.oems) {
      // YoY
      if (oem.yoy_pct == null) {
        const priorVol = volLookup[`${priorYearPeriod}__${g.segment}__${oem.oem_name}`];
        if (priorVol != null && priorVol > 0) {
          oem.yoy_pct = Math.round(((oem.volume - priorVol) / priorVol) * 1000) / 10;
        }
      }
      // MoM
      const priorMoVol = volLookup[`${priorMo}__${g.segment}__${oem.oem_name}`];
      if (priorMoVol != null && priorMoVol > 0) {
        oem.mom_pct = Math.round(((oem.volume - priorMoVol) / priorMoVol) * 1000) / 10;
      }
    }
  }

  // Build monthly data with total-level YoY and MoM
  const monthlyData: FADAMonthData[] = Object.values(groups).map((g) => {
    const [yearStr, monthStr] = g.period.split("-");
    const priorYearPeriod = `${Number(yearStr) - 1}-${monthStr}`;
    const priorMo = priorMonth(g.period);
    const priorYearTotal = totalLookup[`${priorYearPeriod}__${g.segment}`];
    const priorMoTotal = totalLookup[`${priorMo}__${g.segment}`];

    return {
      report_period: g.period,
      segment: g.segment,
      oems: g.oems,
      total_volume: g.total,
      total_yoy_pct: priorYearTotal && priorYearTotal > 0
        ? Math.round(((g.total - priorYearTotal) / priorYearTotal) * 1000) / 10
        : null,
      total_mom_pct: priorMoTotal && priorMoTotal > 0
        ? Math.round(((g.total - priorMoTotal) / priorMoTotal) * 1000) / 10
        : null,
    };
  });

  // ── Build FY aggregates ──
  // Track group→children structure from monthly consolidated data
  const fyGroupChildren: Record<string, Record<string, Record<string, number>>> = {};
  // key: fyKey → groupName → childName → volume

  const fyGroups: Record<string, {
    fy: string; segment: string;
    oemVolumes: Record<string, number>;
    total: number; monthsSet: Set<string>;
    groupNames: Set<string>;
  }> = {};

  for (const g of Object.values(groups)) {
    const fy = periodToFY(g.period);
    const fk = `${fy}__${g.segment}`;
    if (!fyGroups[fk]) {
      fyGroups[fk] = { fy, segment: g.segment, oemVolumes: {}, total: 0, monthsSet: new Set(), groupNames: new Set() };
      fyGroupChildren[fk] = {};
    }
    fyGroups[fk].total += g.total;
    fyGroups[fk].monthsSet.add(g.period);
    for (const oem of g.oems) {
      fyGroups[fk].oemVolumes[oem.oem_name] = (fyGroups[fk].oemVolumes[oem.oem_name] || 0) + oem.volume;
      if (oem.is_group) {
        fyGroups[fk].groupNames.add(oem.oem_name);
        if (oem.children) {
          if (!fyGroupChildren[fk][oem.oem_name]) fyGroupChildren[fk][oem.oem_name] = {};
          for (const child of oem.children) {
            fyGroupChildren[fk][oem.oem_name][child.oem_name] =
              (fyGroupChildren[fk][oem.oem_name][child.oem_name] || 0) + child.volume;
          }
        }
      }
    }
  }

  // Build FY OEM-level data with market share and FY YoY
  const fyTotalLookup: Record<string, number> = {};
  const fyOemLookup: Record<string, number> = {};
  const fyAvgMonthlyLookup: Record<string, number> = {};
  for (const fg of Object.values(fyGroups)) {
    fyTotalLookup[`${fg.fy}__${fg.segment}`] = fg.total;
    const mc = fg.monthsSet.size;
    fyAvgMonthlyLookup[`${fg.fy}__${fg.segment}`] = mc > 0 ? Math.round(fg.total / mc) : 0;
    for (const [name, vol] of Object.entries(fg.oemVolumes)) {
      fyOemLookup[`${fg.fy}__${fg.segment}__${name}`] = vol;
    }
  }

  const fyData: FADAFYData[] = Object.values(fyGroups).map((fg) => {
    const fyNum = parseInt(fg.fy.replace("FY", ""));
    const priorFY = `FY${String(fyNum - 1).padStart(2, "0")}`;
    const priorTotal = fyTotalLookup[`${priorFY}__${fg.segment}`];
    const monthsCount = fg.monthsSet.size;

    const fk = `${fg.fy}__${fg.segment}`;
    // OEMs are already consolidated at the monthly level; compute market share + YoY + group info
    const oems: FADAFYOemRow[] = Object.entries(fg.oemVolumes)
      .map(([name, vol]) => {
        const priorOemVol = fyOemLookup[`${priorFY}__${fg.segment}__${name}`];
        const isGroup = fg.groupNames.has(name);
        const childMap = fyGroupChildren[fk]?.[name];
        const children: FADAFYOemRow[] | undefined = isGroup && childMap
          ? Object.entries(childMap)
              .map(([childName, childVol]) => {
                const priorChildVol = fyOemLookup[`${priorFY}__${fg.segment}__${childName}`];
                return {
                  oem_name: childName,
                  volume: childVol,
                  market_share_pct: fg.total > 0 ? Math.round((childVol / fg.total) * 1000) / 10 : 0,
                  yoy_pct: priorChildVol && priorChildVol > 0
                    ? Math.round(((childVol - priorChildVol) / priorChildVol) * 1000) / 10
                    : null,
                };
              })
              .sort((a, b) => b.volume - a.volume)
          : undefined;
        return {
          oem_name: name,
          volume: vol,
          market_share_pct: fg.total > 0 ? Math.round((vol / fg.total) * 1000) / 10 : 0,
          yoy_pct: priorOemVol && priorOemVol > 0
            ? Math.round(((vol - priorOemVol) / priorOemVol) * 1000) / 10
            : null,
          ...(isGroup ? { is_group: true, children } : {}),
        };
      })
      .sort((a, b) => b.volume - a.volume);

    return {
      fy: fg.fy,
      segment: fg.segment,
      oems,
      total_volume: fg.total,
      months_count: monthsCount,
      avg_monthly: monthsCount > 0 ? Math.round(fg.total / monthsCount) : 0,
      avg_monthly_yoy_pct: (() => {
        const curAvg = monthsCount > 0 ? fg.total / monthsCount : 0;
        const priorAvg = fyAvgMonthlyLookup[`${priorFY}__${fg.segment}`];
        return priorAvg && priorAvg > 0
          ? Math.round(((curAvg - priorAvg) / priorAvg) * 1000) / 10
          : null;
      })(),
      total_yoy_pct: priorTotal && priorTotal > 0
        ? Math.round(((fg.total - priorTotal) / priorTotal) * 1000) / 10
        : null,
    };
  });

  // ── Collect metadata ──
  const allPeriods = [...new Set(monthlyData.map((d) => d.report_period))].sort().reverse();
  const allSegments = [...new Set(monthlyData.map((d) => d.segment))].sort();
  const allFYs = [...new Set(fyData.map((d) => d.fy))].sort().reverse();

  return {
    segments: allSegments,
    availableMonths: allPeriods,
    availableFYs: allFYs,
    latestMonth: allPeriods[0] || "",
    latestFY: allFYs[0] || "",
    monthlyData,
    fyData,
  };
}
