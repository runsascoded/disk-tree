'use client'

import type { ScanDetails } from "@/src/scan-details"
import { Scanned, Time } from "@/components/time"
import { Size, sizeStr } from "@/components/size"
import { FaFileAlt, FaFolder } from "react-icons/fa"
import { Plot } from "@/components/plot"
import { basename } from "path"
import Link from "next/link"
import { Fragment } from "react";
import css from "./scan-details.module.scss"

export function BreadcrumbsPath({ uri, }: { uri: string }) {
  let ancestorUrlPrefix = `/file`
  let isS3 = false
  if (uri.startsWith('s3://')) {
    ancestorUrlPrefix = '/s3'
    uri = uri.slice('s3://'.length)
    isS3 = true
  }
  const segments = (
    uri
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
      return <Fragment key={idx}>
        {
          idx === 0 && isS3
            ? <span className={`${css.separator}`}>s3://</span>
            : <span className={`${css.separator}`}>/</span>
        }
        {
          idx + 1 === segments.length
            ? <span className={`${css.current}`}>{basename(segment)}</span>
            : <Link prefetch href={`${ancestorUrlPrefix}/${segment}`}>
              {basename(segment)}
            </Link>
        }
      </Fragment>
    })}
  </div>
}

export function ScanDetails({ root, children, rows, time }: ScanDetails) {
  const now = new Date()
  const data = [ root, ...rows ]
  const { uri } = root
  console.log({ root, children, rows, time })
  const childUrlPrefix = uri.startsWith('s3://') ? `/s3/${uri.slice('s3://'.length)}` : `/file${uri}`
  return <div>
    <h1><BreadcrumbsPath uri={uri} /></h1>
    <div>
      <table className={css.table}>
        <thead>
        <tr>
          <th></th>
          <th>Path</th>
          <th className={css.size}>Size</th>
          <th className={css.mtime}>Modified</th>
          <th className={css.n_children}>Children</th>
          <th className={css.n_desc}>Desc.</th>
          <th>Scanned</th>
        </tr>
        </thead>
        <tbody>
        {[ root, ...children ].map(({ path, size, mtime, n_desc, n_children, kind, }, idx) => (
          <tr key={path} className={idx === 0 ? "root" : ""}>
            <td>{kind === 'file' ? <FaFileAlt /> : <FaFolder />}</td>
            <td className={css.path}>{
              idx === 0
                ? <code>.</code>
                : <Link prefetch href={`${childUrlPrefix}/${path}`} title={path}>
                  <code>{path}</code>
                </Link>
            }</td>
            <td className={css.size}><Size size={size} /></td>
            <td className={css.mtime}><Time time={mtime * 1000} now={now} /></td>
            <td className={css.n_children}>{n_children ? n_children.toLocaleString() : null}</td>
            <td className={css.n_desc}>{n_desc > 1 ? n_desc.toLocaleString() : null}</td>
            <td className={css.scanned}>
              <Scanned
                time={time}
                now={now}
                onRefresh={() => {
                  console.log("Refresh:", path)
                }}
              />
            </td>
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
