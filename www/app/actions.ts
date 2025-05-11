'use server'

import { Scan } from "./db"
import { db } from "@/app/db";

export async function getScans(): Promise<Scan[]> {
  const stmt = db.prepare<[], Scan>('SELECT * FROM scan')
  return stmt.all()
}
