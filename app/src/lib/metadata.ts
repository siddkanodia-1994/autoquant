/**
 * AutoQuant — Response Metadata & Disclaimers.
 *
 * Standardized provenance, disclaimers, and data freshness
 * metadata for all API responses.
 */

export interface ResponseMeta {
  generated_at: string;
  data_source: string;
  disclaimer?: string;
  coverage: string;
  refresh_interval: string;
}

/** Standard provenance for VAHAN live data */
export function vahanMeta(refreshInterval: string = "1h"): ResponseMeta {
  return {
    generated_at: new Date().toISOString(),
    data_source: "VAHAN (Ministry of Road Transport & Highways, Govt. of India)",
    coverage: "Jan 2016 – Present",
    refresh_interval: refreshInterval,
  };
}

/** Revenue disclaimer (mandatory on all revenue-related endpoints) */
export const REVENUE_DISCLAIMER =
  "IMPORTANT: These figures are demand-based proxies computed as " +
  "registrations × assumed Average Selling Prices (ASPs). This is NOT " +
  "accounting revenue and should not be used for investment decisions. " +
  "Actual OEM revenues depend on wholesale dispatches, exports, pricing, " +
  "discounts, and product mix — none of which are captured here.";

/** Historical data caveat */
export const HISTORICAL_DISCLAIMER =
  "Historical data (2016-2024) sourced from SIAM publications with " +
  "varying granularity. Monthly estimates may be interpolated from " +
  "quarterly or annual aggregates. Confidence levels indicate data " +
  "completeness: HIGH = daily VAHAN feed, MEDIUM = 10+ months SIAM " +
  "coverage, LOW = sparse/estimated.";

/** General data disclaimer for all endpoints */
export const GENERAL_DISCLAIMER =
  "Data provided for informational purposes only. Registration volumes " +
  "are sourced from public government databases and may be subject to " +
  "revision. OEM-level data is mapped from raw registration records " +
  "using a maintained alias table; unmapped registrations are grouped " +
  "under 'Others/Unlisted'.";
