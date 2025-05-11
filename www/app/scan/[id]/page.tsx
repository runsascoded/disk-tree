import { getScan } from "@/app//scan//[id]/actions"

export default async function Home({ params }: any) {
  let { id } = await params
  id = parseInt(id)
  const scan = await getScan(id)
  console.log("scan:", scan)
  return <div>
    <div>Scan {id}</div>
    <div>Path: {scan?.path}</div>
  </div>
}
