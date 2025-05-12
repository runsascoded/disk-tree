import { getScan } from "@/app/scan/[id]/actions"
import { Time } from "@/app/time"
import { Size, sizeStr } from "@/app/size"
import { FaFileAlt, FaFolder } from "react-icons/fa"
import { Plot } from "@/components/plot";
import { basename } from "path";

export default async function Home({ params }: any) {
  let { id } = await params
  id = parseInt(id)
  const res = await getScan(id)
  console.log("scan:", res)
  if (!res) {
    return <div>Scan not found</div>
  }

  const { scan, root, children, rows } = res
  const now = new Date()
  // const parents = new Set(rows.map(({ parent }) => parent))
  const data = [ root, ...rows ]
  return <div>
    <h1>{scan.path}</h1>
    <div>
      <table>
        <thead>
          <tr>
            <th></th>
            <th>Path</th>
            <th>Size</th>
            <th>Modified</th>
            <th># Desc.</th>
            <th># Children</th>
          </tr>
        </thead>
        <tbody>
          {[ root, ...children ].map(({  path, size, mtime, n_desc, n_children, kind, }, idx) => (
            <tr key={path} className={idx === 0 ? "root" : ""}>
              <td>{kind === 'file' ? <FaFileAlt /> : <FaFolder />}</td>
              <td><code>{path}</code></td>
              <td><Size size={size} /></td>
              <td><Time time={mtime * 1000} now={now} /></td>
              <td>{n_desc}</td>
              <td>{n_children}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <Plot
        className={"treemap"}
        // clickHandler={() => {
        //   console.log("clicked", arguments)
        // }}
        data={[{
          type: 'treemap',
          name: '',
          branchvalues: 'total',
          ids: data.map(({ path }) => path),
          labels: data.map(({ path }) => basename(path)),
          parents: data.map(({ path, parent }) => path === root.path ? '' : (parent ?? root.path)),
          values: data.map(({ path, size }) => size),
          text: data.map(({ size }) => sizeStr(size)),
          texttemplate: `%{label}<br>%{text}`,
          customdata: data.map(({ size }) => size),
          hovertemplate: `%{label}<br>%{customdata} bytes<br>%{text}`,
        }]}
        layout={{
          paper_bgcolor: 'transparent',
          margin: { t: 0, r: 0, b: 0, l: 0 },
          // height: 600,
        }}
        config={{
          autosizable: true,
          displayModeBar: false,
          responsive: true,
        }}
      />
    </div>
  </div>
}
