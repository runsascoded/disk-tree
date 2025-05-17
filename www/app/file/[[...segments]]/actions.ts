import { db, Scan } from "@/app/db"
import { scanDetails, ScanDetails } from "@/src/scan-details"

export async function getScan(path: string): Promise<ScanDetails | undefined> {
  if (path.endsWith("/")) {
    path = path.slice(0, -1)
  }
  const path0 = path
  if (path.startsWith('/')) {
    path = path.slice(1)
  }
  const ancestors =
    path.split('/')
      .slice(0, -1)
      .reduce(
        (ancestors, segment) => {
          const prv = ancestors[ancestors.length - 1]
          const nxt = `${prv}${prv === "/" ? "" : "/"}${segment}`
          ancestors.push(nxt)
          return ancestors
        },
        ["/"]
      )
  console.log("ancestors", ancestors)
  // Get most recent scan that matches the path or is for a path that begins with one of the ancestor dir paths
  const ancestorConditions = ancestors.map(() => "path = ?").join(" OR ");
  const query = `
    SELECT *
    FROM scan
    WHERE path = ? OR (${ancestorConditions})
    ORDER BY time DESC
    limit 1
  `;
  const params: (string)[] = [path0, ...ancestors]
  const stmt = db.prepare<string[], Scan>(query)
  const scan = stmt.get(...params)
  if (!scan) return undefined
  return scanDetails(path0, scan)
}
