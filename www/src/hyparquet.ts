import * as fs from "node:fs"
import { ReadStream } from "node:fs"
import { AsyncBuffer } from "hyparquet"

/**
 * Convert a node ReadStream to ArrayBuffer.
 *
 * Copied from hyparquet/src/utils.ts, to work around https://github.com/hyparam/hyparquet/issues/39.
 */
function readStreamToArrayBuffer(input: ReadStream): Promise<ArrayBuffer> {
  return new Promise((resolve, reject) => {
    /** @type {Buffer[]} */
    const chunks = [] as Buffer[]
    input.on('data', chunk => chunks.push(chunk as Buffer))
    input.on('end', () => {
      const buffer = Buffer.concat(chunks)
      resolve(buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength))
    })
    input.on('error', reject)
  })
}

/**
 * Construct an AsyncBuffer for a local file using node fs package.
 *
 * Copied from hyparquet/src/utils.ts, to work around https://github.com/hyparam/hyparquet/issues/39.
 */
export async function asyncBufferFromFile(filename: string): Promise<AsyncBuffer> {
  const stat = await fs.promises.stat(filename)
  return {
    byteLength: stat.size,
    async slice(start: number, end?: number) {
      // read file slice
      const readStream = fs.createReadStream(filename, { start, end })
      return await readStreamToArrayBuffer(readStream)
    },
  }
}
