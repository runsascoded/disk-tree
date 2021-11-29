import {Link} from "react-router-dom";
import React, {useEffect, useState} from "react";
import {Row} from "./data";
import {Worker} from "./worker";
import Plot from "react-plotly.js";
import {SunburstPlotDatum} from "plotly.js";
import {basename, renderSize} from "./utils";
import {numericQueryState, stringQueryState, useQueryState} from "./search-params";

const { fromEntries } = Object

export function DiskTree({ url, worker, dataRoot, }: { url: string, worker: Worker, dataRoot?: string }) {
    const [ data, setData ] = useState<Row[]>([])
    const [ maxNodes, setMaxNodes ] = useQueryState('limit', numericQueryState(1000))
    const [ missingParents, setMissingParents ] = useState<string[]>([])
    const [ root, setRoot ] = useState<string | null>(null)
    useEffect(
        () => {
            if (maxNodes === null) return
            console.log(`Fetching up to ${maxNodes} nodes`)
            worker.fetch<Row>({
                table: 'file',
                limit: maxNodes,
                sorts: [{ column: 'size', desc: true, }],
            }).then(setData)
        },
        [ url, worker, maxNodes, ],
    )

    const [ viewRoot, setViewRoot ] = useQueryState('path', stringQueryState)

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
    // const pad = 20
    const margin = { t: 20, l: 20, b: 20, r: 20, }
    return <>
        <div className="row no-gutters disk-tree meta">
            <div className="col-md-12">
                <span className="control db-url">
                    <label>DB:</label>
                    {url}
                </span>
                <span className="control db-path">
                    <label>Path:</label>
                    <Link to={{ pathname: '/', search: viewRoot ? `?search=${viewRoot}` : undefined, }}>
                        {viewRoot || ''}
                    </Link>
                </span>
                <span className="control max-nodes">
                    <label>Max nodes:</label>
                    <input
                        type="text"
                        className="max-nodes-input"
                        defaultValue={maxNodes || ""}
                        onChange={e => {
                            const value = e.target.value
                            const num = parseInt(value)
                            if (!isNaN(num) && num > 0) {
                                setMaxNodes(num)
                            }
                        }}
                    />
                    {data.length ? ` (actual: ${data.length})` : ""}
                </span>
                {
                    missingParents ?
                        <span className="control missing-parents">
                            Missing parents: {missingParents.join(',')}
                        </span> :
                        ""
                }
            </div>
        </div>
        <Plot
            data={rendered}
            useResizeHandler
            layout={{ autosize: true, margin }}
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
