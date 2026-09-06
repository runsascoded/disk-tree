/** Just enough of `R2Bucket` for the Functions, over a local directory:
 *  `list` / `head` / `get` (with ranges), keyed as `<prefix><filename>`. */
import { readFileSync, readdirSync, statSync } from 'node:fs'
import { join } from 'node:path'

export function dirBucket(dir: string, prefix: string): R2Bucket {
  const path = (key: string) => join(dir, key.slice(prefix.length))
  const bucket = {
    async list(opts?: { prefix?: string; cursor?: string }) {
      const want = opts?.prefix ?? ''
      const objects = readdirSync(dir)
        .filter(f => statSync(join(dir, f)).isFile())
        .map(f => ({ key: prefix + f, size: statSync(join(dir, f)).size }))
        .filter(o => o.key.startsWith(want))
      return { objects, truncated: false, cursor: undefined, delimitedPrefixes: [] }
    },
    async head(key: string) {
      try {
        return { key, size: statSync(path(key)).size }
      } catch {
        return null
      }
    },
    async get(key: string, opts?: { range?: { offset: number; length: number } }) {
      let buf: Buffer
      try {
        buf = readFileSync(path(key))
      } catch {
        return null
      }
      if (opts?.range) buf = buf.subarray(opts.range.offset, opts.range.offset + opts.range.length)
      const bytes = new Uint8Array(buf)  // copy: a Buffer's ArrayBuffer may be a shared pool
      return {
        key,
        size: bytes.byteLength,
        async arrayBuffer() { return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) },
        async text() { return new TextDecoder().decode(bytes) },
      }
    },
  }
  return bucket as unknown as R2Bucket
}
