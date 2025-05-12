import { db, Scan } from "@/app/db"
import { asyncBufferFromFile, parquetReadObjects } from "hyparquet"
import { mapValues } from "@rdub/base"

export type Kind = 'file' | 'dir'

export type Row = {
  path: string
  size: number
  mtime: number
  n_desc: number
  n_children: number
  kind: Kind
  parent: string | null
}

export type ScanDetails = {
  scan: Scan
  root: Row
  children: Row[]
  rows: Row[]
}

export async function scanDetails(scan: Scan): Promise<ScanDetails> {
  let prefix = scan.path
  if (!prefix.endsWith('/')) {
    prefix += '/'
  }

  const { blob } = scan
  const file = await asyncBufferFromFile(blob)
  let rows =
    (await parquetReadObjects({ file }))
      .map(
        row => mapValues(
          row,
          (_, v) => typeof v === 'bigint' ? parseInt(v) : v
        )
      ) as Row[]
  const levels = 2
  const [ root, ...rest ] = rows
  if (root.path != scan.path) {
    throw new Error(`Root path ${root.path} doesn't match scan path ${scan.path}`)
  }
  root.parent = null
  console.log("root:", root)
  const children: Row[] = []
  rows = rest.map(({ path, parent, ...obj }) => {
    if (!path.startsWith(prefix)) {
      throw new Error(`Path ${path} doesn't start with ${prefix}`)
    }
    path = path.slice(prefix.length)
    if (parent === root.path) {
      parent = null
    } else if (parent?.startsWith(prefix)) {
      parent = parent.slice(prefix.length)
    } else {
      throw new Error(`Path ${path}'s parent ${parent} doesn't start with ${prefix}`)
    }
    return { path, parent, ...obj }
  }).filter(({ path, ...obj }) =>{
    const depth = path.split('/').length
    if (depth === 1) {
      children.push({ path, ...obj })
    }
    return depth <= levels
  })
  return { scan, root, children, rows }
}

export async function getScan(id: number): Promise<ScanDetails | undefined> {
  const stmt = db.prepare<[number], Scan>('SELECT * FROM scan WHERE id = ?')
  const scan = stmt.get(id)
  if (!scan) return undefined
  return scanDetails(scan)
}
