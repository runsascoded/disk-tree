import { getScans } from "@/app/actions"
import Link from "next/link"
import { Time } from "@/app/time";

export default async function Home() {
  const scans = await getScans()
  console.log("scans:", scans)
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
            {scans.map((scan) => (
              <tr key={scan.id}>
                <td><Link href={`/file/${scan.path}`}>{scan.path}</Link></td>
                <td><Time time={scan.time} now={now} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </main>
    </div>
  );
}
