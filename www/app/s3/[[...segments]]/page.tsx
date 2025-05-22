import { getScan } from "@/app/file/[[...segments]]/actions"
import Home from "@/app/page"
import { BreadcrumbsPath, ScanDetails } from "@/components/scan-details"

export default async function Page(
  { params }: {
    params: Promise<{ segments: string[] | undefined }>
  }
) {
  let { segments } = await params
  segments = (segments ?? []).map(s => decodeURIComponent(s))
  console.log("s3/[[...segments]]:", segments)
  const uri = `s3://${segments.join('/')}`
  const res = await getScan(uri)
  if (!res) {
    return <div>
      <h1><BreadcrumbsPath uri={uri} /></h1>
      <div>Scan not found</div>
      <Home />
    </div>
  }
  return <ScanDetails {...res} />
}
