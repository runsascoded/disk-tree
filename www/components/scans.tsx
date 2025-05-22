'use client'

import { Scan } from "@/app/db"
import Link from "next/link"
import { Time } from "@/components/time"

export function Scans({ scans }: { scans: Scan[] }) {
  const now = new Date()
  return (
    <div className="grid grid-rows-[20px_1fr_20px] items-center justify-items-center min-h-screen p-8 pb-20 gap-16 sm:p-20 font-[family-name:var(--font-geist-sans)]">
      <main className="flex flex-col gap-[32px] row-start-2 items-center sm:items-start">
        <table>
          <thead>
          <tr>
            <th>Path</th>
            <th>Scanned</th>
          </tr>
          </thead>
          <tbody>
          {scans.map((scan) => {
            const href = scan.path.startsWith('s3://') ? `/s3/${scan.path.slice('s3://'.length)}` : `/file/${scan.path}`
            return (
              <tr key={scan.id}>
                <td><Link prefetch href={href}>{scan.path}</Link></td>
                <td><Time time={scan.time} now={now}/></td>
              </tr>
            )
          })}
          </tbody>
        </table>
      </main>
    </div>
  )
}
