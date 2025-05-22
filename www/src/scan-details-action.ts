'use server'

import { dirname } from "path"
import { mapValues } from "@rdub/base"
import { parquetReadObjects } from "hyparquet"
import { Scan } from "@/app/db"
import { Row, ScanDetails } from "@/src/scan-details"
import { asyncBufferFromFile } from "./hyparquet"

export async function scanDetails(uri: string, scan: Scan): Promise<ScanDetails> {
  let prefix = uri
  if (uri.endsWith("/")) {
    uri = uri.slice(0, -1)
  } else {
    prefix += "/"
  }

  const { id, blob, time } = scan
  console.log(`${uri}: scan ${id}, ${blob}`)
  const file = await asyncBufferFromFile(blob)
  // let rows = await parquetReadObjects({ file }) as Row[]
  // console.log(`${uri}: ${rows.length} rows from parquet, prefix ${prefix}`)
  // rows = (
  //   rows
  let rows = (
    (await parquetReadObjects({ file }))
      .filter(
        ({ uri: u }) => u.startsWith(prefix) || u === uri
      )
      .map(row => {
        if (scan.path === uri) {
          return row
        }
        let { path, parent, ...obj } = row
        const uriPrefix = `${uri}/`
        if (row.uri === uri) {
          path = '.'
          parent = null
        } else if (!row.uri.startsWith(uriPrefix)) {
          throw new Error(`${uri}: ${row.uri} doesn't start with scan URI prefix ${uriPrefix}`)
        } else {
          path = row.uri.slice(uriPrefix.length)
          parent = (parent === uri) ? '.' : dirname(path)
        }
        return { path, parent, ...obj }
      })
      .map(
        row => mapValues(
          row,
          (_, v) => typeof v === 'bigint' ? parseInt(v as any) : v
        )
      ) as Row[]
  )
  const levels = 2
  const [ root, ...rest ] = rows
  if (root.path != '.') {
    throw new Error(`${uri}: unexpected root path`)
  }
  root.parent = null
  console.log("root:", root)
  const children: Row[] = []
  rows = rest.filter(({ path, ...obj }) => {
    const depth = path.split('/').length
    if (depth === 1 && path !== '.') {
      children.push({ path, ...obj })
    }
    return depth <= levels
  })
  return { root, children, rows, time }
}
