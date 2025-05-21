'use server'

import { Scan } from "./db"
import { db } from "@/app/db"

export async function getScans(): Promise<Scan[]> {
  const stmt = db.prepare<[], Scan>(`
    SELECT s.*
    FROM scan s
    INNER JOIN (
      SELECT id, MAX(time) as max_time
      FROM scan
      GROUP BY path
    ) latest
    ON s.id = latest.id
    ORDER BY s.time DESC
  `)
  return stmt.all()
}
