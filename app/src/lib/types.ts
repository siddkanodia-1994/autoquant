/**
 * AutoQuant — Shared TypeScript Types.
 */

// ── Dimension Types ──
export interface OEM {
  oem_id: number;
  oem_name: string;
  nse_ticker: string | null;
  bse_code: string | null;
  is_listed: boolean;
  is_in_scope: boolean;
  primary_segments: string[];
}

export interface Segment {
  segment_id: number;
  segment_code: "PV" | "CV" | "2W";
  segment_name: string;
  sub_segment: string | null;
}

export interface Fuel {
  fuel_id: number;
  fuel_code: string;
  powertrain: "ICE" | "EV" | "HYBRID";
  dashboard_bucket: "ICE" | "EV";
  fuel_group: string;
}

// ── API Response Types ──
export interface IndustryPulseData {
  latestDate: string;
  latestDayTotal: number;
  mtdTotal: number;
  qtdTotal: number;
  ytdTotal: number;
  mtdYoY: number;
  qtdYoY: number;
  ytdYoY: number;
  segmentBreakdown: SegmentShare[];
  dailySeries: DailyDataPoint[];
  evPenetration: EVPenetrationPoint[];
  fuelMixTrend: FuelMixPoint[];
  dataFreshness: string;
}

export interface SegmentShare {
  segment: string;
  value: number;
  percentage: number;
}

export interface DailyDataPoint {
  date: string;
  pv: number;
  cv: number;
  tw: number;
  total: number;
  ma7: number; // 7-day moving average
}

export interface EVPenetrationPoint {
  month: string;
  pv_ev_pct: number;
  cv_ev_pct: number;
  tw_ev_pct: number;
}

export interface FuelMixPoint {
  month: string;
  petrol: number;
  diesel: number;
  cng: number;
  hybrid: number;
  ev: number;
  other: number;
}

export interface OEMDeepDiveData {
  oem: OEM;
  mtdVolume: number;
  qtdVolume: number;
  ytdVolume: number;
  mtdYoY: number;
  qtdYoY: number;
  ytdYoY: number;
  mtdMoM: number;
  marketShareTrend: MarketSharePoint[];
  iceEvSplit: ICEvsEVPoint[];
  segmentBreakdown: SegmentBreakdownPoint[];
}

export interface MarketSharePoint {
  month: string;
  share_pct: number;
  segment: string;
}

export interface ICEvsEVPoint {
  month: string;
  ice: number;
  ev: number;
}

export interface SegmentBreakdownPoint {
  month: string;
  pv: number;
  cv: number;
  tw: number;
}

// ── Scorecard Types ──
export interface ScorecardRow {
  oem_name: string;
  nse_ticker: string;
  segment: string;
  qtd_volume: number;
  yoy_pct: number;
  market_share_pct: number;
  ev_pct: number;
  est_rev_cr: number | null;
  confidence: string;
}

// ── Historical Data Types ──
export interface HistoricalYearSummary {
  year: number;
  fy_label: string;
  total_registrations: number;
  pv_registrations: number;
  cv_registrations: number;
  tw_registrations: number;
  ev_registrations: number;
  ev_pct: number;
  yoy_pct: number | null;
  oems_with_data: number;
  data_source: string;
  confidence: "HIGH" | "MEDIUM" | "LOW";
  months_with_data: number;
}

export interface HistoricalMonthPoint {
  month: string; // YYYY-MM
  pv: number;
  cv: number;
  tw: number;
  total: number;
  ev_total: number;
  source: string;
}

export interface HistoricalData {
  yearSummaries: HistoricalYearSummary[];
  monthlyTrend: HistoricalMonthPoint[];
  dataRange: { min: string; max: string };
  totalRecords: number;
  coveragePct: number;
}

// ── Revenue Estimator Types ──
export interface RevenueEstimate {
  oem_name: string;
  nse_ticker: string;
  fy_quarter: string;
  reg_volume: number;
  est_domestic_rev_cr: number;
  est_rev_low_cr: number;
  est_rev_high_cr: number;
  data_completeness_pct: number;
}

// ── FADA Retail Data Types ──
export interface FADAOemRow {
  oem_name: string;
  volume: number;
  market_share_pct: number;
  yoy_pct: number | null;
}

export interface FADAMonthData {
  report_period: string; // YYYY-MM
  segment: string;       // 2W | CV | PV
  oems: FADAOemRow[];
  total_volume: number;
}

export interface FADADashboardData {
  segments: string[];
  availableMonths: string[];
  latestMonth: string;
  data: FADAMonthData[];
}
