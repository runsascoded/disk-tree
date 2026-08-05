/**
 * URI-scheme / route-type helpers.
 *
 * disk-tree URLs come in two flavors:
 *   - `/file/<local-path>` for local filesystem scans
 *   - `/<scheme>/<host-and-path>` for URI-scheme scans (s3/gcs/r2/ssh)
 *
 * The URIs on the backend / API use the canonical form: `/local/path` or
 * `<scheme>://host/path`. Keep every scheme<->route-type mapping in this file
 * so adding a new scheme (e.g. `azure`) is a one-line edit.
 */

/** All URI schemes recognized in disk-tree URLs. `file` denotes a local path. */
export type RouteType = 'file' | 's3' | 'gcs' | 'r2' | 'ssh'

/** Schemes rendered as `<scheme>://…` URIs (everything except `file`). */
export const URI_SCHEMES: readonly Exclude<RouteType, 'file'>[] = ['s3', 'gcs', 'r2', 'ssh']

/** Detect the route type from a `location.pathname` (e.g. `/s3/bucket`). */
export function detectRouteType(pathname: string): RouteType {
  for (const scheme of URI_SCHEMES) {
    if (pathname === `/${scheme}` || pathname.startsWith(`/${scheme}/`)) return scheme
  }
  return 'file'
}

/**
 * Reconstruct a URI ('gcs://bucket/prefix' or '/local/path') from a route
 * type and the '*' segments captured by react-router's splat route.
 */
export function segmentsToUri(routeType: RouteType, segments: string): string {
  return routeType === 'file' ? `/${segments}` : `${routeType}://${segments}`
}

/** URI → browser path. `s3://foo/bar` → `/s3/foo/bar`, `/x/y` → `/file/x/y`. */
export function uriToPath(uri: string): string {
  for (const scheme of URI_SCHEMES) {
    const prefix = `${scheme}://`
    if (uri.startsWith(prefix)) return `/${scheme}/${uri.slice(prefix.length)}`
  }
  return `/file${uri}`
}

/**
 * Prefix for building child-links under `uri` (no trailing slash).
 * `/`         → `/file`
 * `s3://`     → `/s3`
 * `s3://b/x/` → `/s3/b/x`
 */
export function childLinkPrefix(uri: string): string {
  const path = uriToPath(uri).replace(/\/+$/, '')
  return path || '/file'
}

/**
 * Reconstruct a URI from a `/compare/<scheme>/<segments>` pathname.
 * The caller has already stripped the leading `/compare`. Returns
 * `('/', 'file')` for the empty case.
 */
export function comparePathToUri(pathAfterCompare: string): { uri: string; routeType: RouteType } {
  for (const scheme of URI_SCHEMES) {
    if (pathAfterCompare === `/${scheme}` || pathAfterCompare.startsWith(`/${scheme}/`)) {
      const rest = pathAfterCompare.slice(scheme.length + 1) // strip '/<scheme>'
      const uri = rest ? `${scheme}:/${decodeURIComponent(rest)}` : `${scheme}://`
      return { uri, routeType: scheme }
    }
  }
  if (pathAfterCompare.startsWith('/file')) {
    return { uri: decodeURIComponent(pathAfterCompare.replace(/^\/file/, '') || '/'), routeType: 'file' }
  }
  return { uri: decodeURIComponent(pathAfterCompare || '/'), routeType: 'file' }
}

/** `true` for `'/'` or `<scheme>://` (empty-scan-root placeholder URIs). */
export function isSchemeRoot(uri: string): boolean {
  if (uri === '/') return true
  return URI_SCHEMES.some(s => uri === `${s}://`)
}

/**
 * Whether the delete UI should render for a route type. Historical: the S3
 * button is hidden even though the S3 backend supports it — mirror that for
 * gcs/r2 which don't yet have DT backends.
 */
export function supportsDelete(routeType: RouteType): boolean {
  return routeType === 'file' || routeType === 'ssh'
}
