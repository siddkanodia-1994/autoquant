# AutoQuant — India Auto Registrations & Demand Dashboard

Live-tracking data product that ingests daily vehicle registration volumes from India's VAHAN dashboard, classifies into PV/CV/2W × ICE/EV, maps to listed Auto OEMs, and computes demand-based implied revenue proxies.

## Stack

- **Frontend**: Next.js 16 + Tailwind 4 + Recharts 3
- **Backend**: Python ETL (Playwright + pdfplumber + httpx)
- **Database**: Supabase PostgreSQL (Bronze/Silver/Gold architecture)
- **Deploy**: Vercel + GitHub Actions

## Structure

```
app/          → Next.js dashboard (Vercel)
etl/          → Python ETL pipeline
step1/        → DDL + seed SQL
```

## Coverage

- 15 listed OEMs + BYD India + Others/Unlisted
- January 2016 → present
- Sources: VAHAN, FADA, BSE filings, SIAM historical
