"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  BarChart3,
  Car,
} from "lucide-react";
import { cn } from "@/lib/utils";

const navItems: {
  href: string;
  label: string;
  icon: typeof Car;
  disabled?: boolean;
}[] = [
  { href: "/fada", label: "FADA Retail", icon: Car },
];

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="fixed left-0 top-0 z-30 flex h-screen w-60 flex-col border-r border-zinc-200 bg-white dark:border-zinc-800 dark:bg-zinc-950">
      {/* Brand */}
      <div className="flex items-center gap-2 px-5 py-5 border-b border-zinc-100 dark:border-zinc-800">
        <BarChart3 className="h-6 w-6 text-blue-600" />
        <span className="text-lg font-bold tracking-tight text-zinc-900 dark:text-zinc-100">
          AutoQuant
        </span>
      </div>

      {/* Navigation */}
      <nav className="flex-1 space-y-1 px-3 py-4">
        {navItems.map((item) => {
          const isActive =
            pathname === item.href || pathname?.startsWith(item.href + "/");
          const Icon = item.icon;

          if (item.disabled) {
            return (
              <div
                key={item.href}
                className="flex items-center gap-3 rounded-lg px-3 py-2 text-sm text-zinc-400 cursor-not-allowed"
              >
                <Icon className="h-4 w-4" />
                <span>{item.label}</span>
                <span className="ml-auto rounded bg-zinc-100 px-1.5 py-0.5 text-[10px] font-medium text-zinc-400 dark:bg-zinc-800">
                  Soon
                </span>
              </div>
            );
          }

          return (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors",
                isActive
                  ? "bg-blue-50 text-blue-700 dark:bg-blue-950 dark:text-blue-400"
                  : "text-zinc-600 hover:bg-zinc-50 hover:text-zinc-900 dark:text-zinc-400 dark:hover:bg-zinc-900 dark:hover:text-zinc-100"
              )}
            >
              <Icon className="h-4 w-4" />
              <span>{item.label}</span>
            </Link>
          );
        })}
      </nav>

      {/* Footer */}
      <div className="border-t border-zinc-100 px-5 py-3 dark:border-zinc-800">
        <p className="text-[10px] text-zinc-400">
          Data: VAHAN &bull; FADA &bull; Updated daily
        </p>
      </div>
    </aside>
  );
}
