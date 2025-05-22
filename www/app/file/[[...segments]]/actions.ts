import { db, Scan } from "@/app/db"
import { ScanDetails } from "@/src/scan-details"
import { scanDetails } from "@/src/scan-details-action"

export async function getScan(uri: string): Promise<ScanDetails | undefined> {
  if (uri.endsWith("/")) {
    uri = uri.slice(0, -1)
  }
  const uri0 = uri
  let prefix = ``
  if (uri.startsWith('/')) {
    uri = uri.slice(1)
  } else if (uri.startsWith('s3://')) {
    uri = uri.substring('s3://'.length)
    const m = uri.match(/^([^/]+)\/(.+)$/)!
    let [ _, bkt, key ] = m ?? [ '', uri, '' ]
    uri = key
    prefix = `s3://${bkt}`
    // console.log("key", key, "uri", uri, "prefix", prefix)
  }
  let ancestors =
    uri
      .split('/')
      .slice(0, -1)
      .reduce(
        (ancestors, segment) => {
          const prv = ancestors[ancestors.length - 1]
          const nxt = `${prv}${prv === "/" ? "" : "/"}${segment}`
          ancestors.push(nxt)
          return ancestors
        },
        ["/"]
      ).map(
        ancestor =>
          `${prefix}${ancestor}`.replace(/\/$/, "")
      )
  // console.log("ancestors", ancestors)
  // Get most recent scan that matches the path or is for a path that begins with one of the ancestor dir paths
  const ancestorConditions = ancestors.map(() => "path = ?").join(" OR ")
  const query = `
    SELECT *
    FROM scan
    WHERE path = ? OR (${ancestorConditions})
    ORDER BY time DESC
    limit 1
  `
  const params: (string)[] = [uri0, ...ancestors]
  const stmt = db.prepare<string[], Scan>(query)
  const scan = stmt.get(...params)
  if (!scan) return undefined
  return scanDetails(uri0, scan)
}
