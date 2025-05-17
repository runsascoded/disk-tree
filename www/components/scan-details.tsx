import type { ScanDetails } from "@/src/scan-details"
import { Time } from "@/app/time"
import { Size, sizeStr } from "@/app/size"
import { FaFileAlt, FaFolder } from "react-icons/fa"
import { Plot } from "@/components/plot"
import { basename } from "path"
import Link from "next/link"
import { Fragment } from "react";
import css from "./scan-details.module.scss"

export function BreadcrumbsPath({ path }: { path: string }) {
  const segments = (
    path
      .split('/')
      .filter(Boolean)
      .reduce(
        (acc, segment) => {
          const prev = acc[acc.length - 1]
          const next = prev ? `${prev}/${segment}` : segment
          acc.push(next)
          return acc
        },
        [] as string[]
      )
  )
  console.log("segments", segments)
  return <div className="breadcrumbs">
    {segments.map((segment, idx) => {
      // const href = '/' + segments.slice(0, idx + 1).join('/')
      return <Fragment key={idx}>
        <span className={`${css.separator}`}>/</span>
        {
          idx + 1 === segments.length
            ? <span className={`${css.current}`}>{basename(segment)}</span>
            : <Link href={`/file/${segment}`}>
              {basename(segment)}
            </Link>
        }
      </Fragment>
    })}
  </div>
}

export function ScanDetails({ root, children, rows }: ScanDetails) {
  const now = new Date()
  const data = [ root, ...rows ]
  return <div>
    <h1><BreadcrumbsPath path={root.path} /></h1>
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
        {[ root, ...children ].map(({ path, size, mtime, n_desc, n_children, kind, }, idx) => (
          <tr key={path} className={idx === 0 ? "root" : ""}>
            <td>{kind === 'file' ? <FaFileAlt /> : <FaFolder />}</td>
            <td>{
              idx === 0
                ? <code>{path}</code>
                : <Link href={`/file${root.path}/${path}`}>
                  <code>{path}</code>
                </Link>
            }</td>
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
        data={[{
          type: 'treemap',
          name: '',
          branchvalues: 'total',
          ids: data.map(({ path }) => path),
          labels: data.map(({ path }) => basename(path)),
          parents: data.map(({ path, parent }) => path === root.path ? '' : (parent ?? root.path)),
          values: data.map(({ size }) => size),
          text: data.map(({ size }) => sizeStr(size)),
          texttemplate: `%{label}<br>%{text}`,
          customdata: data.map(({ size }) => size),
          hovertemplate: `%{label}<br>%{customdata} bytes<br>%{text}`,
        }]}
        layout={{
          paper_bgcolor: 'transparent',
          margin: { t: 0, r: 0, b: 0, l: 0 },
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
