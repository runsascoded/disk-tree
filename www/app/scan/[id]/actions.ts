import { db, Scan } from "@/app/db"

export type Row = {
  path: string
  size: number
  mtime: string
  n_desc: number
  n_children: number
}

export type ScanDetails = {
  scan: Scan
  rows: Row[]
}

export async function getScan(id: number): Promise<ScanDetails | undefined> {
  const stmt = db.prepare<[number], Scan>('SELECT * FROM scan WHERE id = ?')
  const scan = stmt.get(id)
  if (!scan) return undefined
  const { blob } = scan
}
