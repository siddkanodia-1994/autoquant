"use client";

import { formatDate } from "@/lib/utils";
import { RefreshCw, Bell } from "lucide-react";

interface HeaderProps {
  title: string;
  subtitle?: string;
  dataDate?: string;
}

export function Header({ title, subtitle, dataDate }: HeaderProps) {
  return (
    <header className="flex items-center justify-between border-b border-zinc-200 bg-white px-6 py-4 dark:border-zinc-800 dark:bg-zinc-950">
      <div>
        <h1 className="text-xl font-bold text-zinc-900 dark:text-zinc-100">
          {title}
        </h1>
        {subtitle && (
          <p className="text-sm text-zinc-500 dark:text-zinc-400">{subtitle}</p>
        )}
      </div>
      <div className="flex items-center gap-4">
        {dataDate && (
          <span className="flex items-center gap-1.5 rounded-lg bg-zinc-50 px-3 py-1.5 text-xs font-medium text-zinc-600 dark:bg-zinc-900 dark:text-zinc-400">
            <RefreshCw className="h-3 w-3" />
            Data as of {formatDate(dataDate)}
          </span>
        )}
        <button className="rounded-lg p-2 text-zinc-400 transition-colors hover:bg-zinc-50 hover:text-zinc-600 dark:hover:bg-zinc-900">
          <Bell className="h-4 w-4" />
        </button>
      </div>
    </header>
  );
}
