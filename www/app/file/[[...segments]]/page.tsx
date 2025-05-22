import Home from "@/app/page"
import { BreadcrumbsPath, ScanDetails } from "@/components/scan-details"
import { getScan } from "./actions"

export default async function Page(
  { params }: {
    params: Promise<{ segments: string[] | undefined }>
  }
) {
  let { segments } = await params
  segments = (segments ?? []).map(s => decodeURIComponent(s))
  console.log("file/[[...segments]]:", segments)
  const uri = segments ? [ '', ...segments ].join('/') : '/'
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
