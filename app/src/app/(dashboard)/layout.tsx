"use client";

import { Sidebar } from "@/components/layout/sidebar";
import { ReactNode } from "react";

export default function DashboardLayout({ children }: { children: ReactNode }) {
  return (
    <div className="min-h-screen">
      <Sidebar />
      <main className="ml-60">{children}</main>
    </div>
  );
}
