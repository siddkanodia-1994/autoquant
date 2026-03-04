import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";

const inter = Inter({ subsets: ["latin"], variable: "--font-inter" });

export const metadata: Metadata = {
  title: "AutoQuant — India Auto Registrations Dashboard",
  description:
    "Live-tracking India vehicle registrations from VAHAN. Segment-wise PV/CV/2W volumes, EV penetration, OEM deep-dives, and demand-based revenue proxies.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className={`${inter.variable} font-sans antialiased bg-zinc-50 text-zinc-900 dark:bg-zinc-950 dark:text-zinc-100`}>
        {children}
      </body>
    </html>
  );
}
