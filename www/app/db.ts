import Database from "better-sqlite3"

export const DB = '/Users/ryan/.config/disk-tree/disk-tree.db'

export const db = new Database(DB, { readonly: true })

export type Scan = {
  id: number
  path: string
  time: string
  blob: string
}
