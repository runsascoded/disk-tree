import { getScans } from "@/app/actions"
import { Scans } from "@/components/scans";

export default async function Home() {
  const scans = await getScans()
  console.log("scans:", scans)
  return <Scans scans={scans} />
}
