import { getScan } from "./actions"
import { ScanDetails } from "@/app/scan/[id]/page"

export default async function Page({ params }: {
  params: Promise<{ segments: string[] | undefined }>
}) {
  const { segments } = await params
  console.log("segments", segments)
  const path = segments ? [ '', ...segments ].join('/') : '/'
  const res = await getScan(path)
  if (!res) {
    return <div>Scan not found</div>
  }
  return <ScanDetails {...res} />
}
