import { getScan } from "./actions"
import { BreadcrumbsPath, ScanDetails } from "@/components/scan-details"
import Home from "@/app/page"

export default async function Page(
  { params }: {
    params: Promise<{ segments: string[] | undefined }>
  }
) {
  const { segments } = await params
  console.log("segments", segments)
  const path = segments ? [ '', ...segments ].join('/') : '/'
  const res = await getScan(path)
  if (!res) {
    return <div>
      <h1><BreadcrumbsPath path={path} /></h1>
      <div>Scan not found</div>
      <Home />
    </div>
  }
  return <ScanDetails {...res} />
}
