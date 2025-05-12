import { db, Scan } from "@/app/db"
import { scanDetails, ScanDetails } from "@/app/scan/[id]/actions"

export async function getScan(path: string): Promise<ScanDetails | undefined> {
  const stmt = db.prepare<[ string ], Scan>('SELECT * FROM scan WHERE path = ? ORDER BY time DESC limit 1')
  const scan = stmt.get(path)
  if (!scan) return undefined
  return scanDetails(scan)
}
