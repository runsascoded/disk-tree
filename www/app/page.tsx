import { getScans } from "@/app/actions"
import Link from "next/link"

export function Time({ time }: { time: string }) {
  const date = new Date(time)
  const options: Intl.DateTimeFormatOptions = {
    year: "numeric",
    month: "numeric",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }
  return <span>{date.toLocaleString("en-US", options)}</span>
}

export default async function Home() {
  const scans = await getScans()
  console.log("scans:", scans)
  return (
    <div className="grid grid-rows-[20px_1fr_20px] items-center justify-items-center min-h-screen p-8 pb-20 gap-16 sm:p-20 font-[family-name:var(--font-geist-sans)]">
      <main className="flex flex-col gap-[32px] row-start-2 items-center sm:items-start">
        <table>
          <thead>
            <tr>
              {/*<th>id</th>*/}
              <th>path</th>
              <th>time</th>
              {/*<th>blob</th>*/}
            </tr>
          </thead>
          <tbody>
            {scans.map((scan) => (
              <tr key={scan.id}>
                {/*<td>{scan.id}</td>*/}
                <td><Link href={`/scan/${scan.id}`}>{scan.path}</Link></td>
                <td><Time time={scan.time} /></td>
                {/*<td>{basename(scan.blob)}</td>*/}
              </tr>
            ))}
          </tbody>
        </table>
      </main>
    </div>
  );
}
