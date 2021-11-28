import {Link} from "react-router-dom";
import React, {useEffect, useState} from "react";
import {Row} from "./data";
import {Worker} from "./worker";
import Plot from "react-plotly.js";
import {SunburstPlotDatum} from "plotly.js";
import {basename, renderSize} from "./utils";
import {queryParamState, stringQueryState} from "./search-params";

const { fromEntries } = Object

export function DiskTree({ url, worker, dataRoot, }: { url: string, worker: Worker, dataRoot?: string }) {
    const [ data, setData ] = useState<Row[]>([])
    const [ limit, setLimit ] = useState(1000)
    const [ missingParents, setMissingParents ] = useState<string[]>([])
    const [ root, setRoot ] = useState<string | null>(null)
    useEffect(
        () => {
            worker.fetch<Row>({
                table: 'file',
                limit,
                sorts: [{ column: 'size', desc: true, }],
            }).then(setData)
        },
        [ url, worker,],
    )

    const [ viewRoot, setViewRoot, queryPath ] = queryParamState('path', stringQueryState)

    useEffect(
        () => {
            const rowDict = fromEntries(data.map(r => [ r.path, r ]))
            const [ root, ...missingParents ] = data.map(r => r.parent).filter(parent => !(parent in rowDict)).sort()
            console.log("root:", root, "missingParents:", missingParents)
            setMissingParents(missingParents)
            setRoot(root)
            if (viewRoot === null && root !== undefined) {
                console.log("setting viewRoot to root:", root)
                setViewRoot(root)
            }
        },
        [ data, ]
    )

    let rendered: Plotly.Data[] = []
    if (data.length) {
        const ids = data.map(r => r.path)
        const labels = data.map(({ path, size, }) => `${root == path ? root : basename(path)}: ${renderSize(size)}`)
        const values = data.map(r => r.size)
        const parents = data.map(r => r.parent)
        rendered = [{
            type: "treemap",
            ids,
            textinfo: 'label',
            text: labels,
            hoverinfo: 'text',
            labels,
            values,
            parents,
            branchvalues: 'total',
            level: viewRoot || "",
    }]
        console.log(rendered)
    }
    const pad = 20
    return <>
        <div className="meta">
            <span className="db-url">
                <label>DB:</label>
                {url}
            </span>
            <span className="db-path">
                <label>Path:</label>
                <Link to={{ pathname: '/', search: viewRoot ? `?search=${viewRoot}` : undefined, }}>
                    {viewRoot || ''}
                </Link>
            </span>
        </div>
        <Plot
            data={rendered}
            useResizeHandler
            layout={{ autosize: true, margin: { t: pad, l: pad, b: pad, r: pad, } }}
            style={{ width: '100%', height: '100%' }}
            config={{ responsive: true }}
            onClick={
                (e) => {
                    const point = e.points[0]
                    try {
                        const datum = point as any as SunburstPlotDatum
                        const { root, id, label, parent } = datum
                        console.log("treemap click:", e, root, id, label, parent)
                        setViewRoot(id === undefined ? root : id)
                    } catch (error) {
                        console.error(error)
                    }
                }
            }
        />
    </>
}
