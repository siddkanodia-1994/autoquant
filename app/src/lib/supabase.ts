/**
 * AutoQuant — Supabase Client Configuration.
 *
 * Server-side: Uses service role key (for API routes)
 * Client-side: Uses anon key (read-only via RLS)
 */

import { createClient } from "@supabase/supabase-js";

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

/** Client-side Supabase (anon key, read-only via RLS) */
export const supabase = createClient(supabaseUrl, supabaseAnonKey, {
  db: { schema: "autoquant" },
});

/** Server-side Supabase (service role, bypasses RLS) */
export function getServiceSupabase() {
  if (!supabaseServiceKey) {
    throw new Error("SUPABASE_SERVICE_ROLE_KEY not set");
  }
  return createClient(supabaseUrl, supabaseServiceKey, {
    db: { schema: "autoquant" },
    auth: { persistSession: false },
  });
}
