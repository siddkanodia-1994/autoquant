"use client";

import { cn, formatIndian, formatPct } from "@/lib/utils";
import { TrendingUp, TrendingDown, Minus } from "lucide-react";

interface KPICardProps {
  title: string;
  value: number | string;
  yoy?: number;
  trend?: number;
  subtitle?: string;
  className?: string;
}

export function KPICard({ title, value, yoy, trend, subtitle, className }: KPICardProps) {
  const trendVal = yoy ?? trend;
  const isPositive = trendVal !== undefined && trendVal > 0;
  const isNegative = trendVal !== undefined && trendVal < 0;

  const displayValue = typeof value === "number" ? formatIndian(value) : value;

  return (
    <div
      className={cn(
        "rounded-xl border border-zinc-200 bg-white p-5 shadow-sm dark:border-zinc-800 dark:bg-zinc-950",
        className
      )}
    >
      <p className="text-xs font-medium uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
        {title}
      </p>
      <p className="mt-2 text-2xl font-bold text-zinc-900 dark:text-zinc-100">
        {displayValue}
      </p>
      {trendVal !== undefined && (
        <div className="mt-1 flex items-center gap-1">
          {isPositive && <TrendingUp className="h-3.5 w-3.5 text-emerald-500" />}
          {isNegative && <TrendingDown className="h-3.5 w-3.5 text-red-500" />}
          {!isPositive && !isNegative && <Minus className="h-3.5 w-3.5 text-zinc-400" />}
          <span
            className={cn(
              "text-xs font-medium",
              isPositive && "text-emerald-600",
              isNegative && "text-red-600",
              !isPositive && !isNegative && "text-zinc-500"
            )}
          >
            {formatPct(trendVal)} YoY
          </span>
        </div>
      )}
      {subtitle && (
        <p className="mt-1 text-xs text-zinc-400">{subtitle}</p>
      )}
    </div>
  );
}
