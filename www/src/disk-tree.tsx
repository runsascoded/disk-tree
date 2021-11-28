import {Link} from "react-router-dom";
import React, {useEffect, useState} from "react";
import {Row} from "./data";
import {Worker} from "./worker";
import Plot from "react-plotly.js";
import {SunburstPlotDatum} from "plotly.js";
import {basename} from "./utils";

const { fromEntries } = Object

export function DiskTree({ url, worker, dataRoot, }: { url: string, worker: Worker, dataRoot?: string }) {
    const [ data, setData ] = useState<Row[]>([])
    const [ limit, setLimit ] = useState(100)
    const [ missingParents, setMissingParents ] = useState<string[]>([])
    const [ root, setRoot ] = useState<string | null>(null)
    const [ viewRoot, setViewRoot ] = useState<string | null>(null)
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

    useEffect(
        () => {
            const rowDict = fromEntries(data.map(r => [ r.path, r ]))
            const [ root, ...missingParents ] = data.map(r => r.parent).filter(parent => !(parent in rowDict)).sort()
            console.log("root:", root, "missingParents:", missingParents)
            setMissingParents(missingParents)
            setRoot(root)
            setViewRoot(root)
        },
        [ data, ]
    )

    let rendered: Plotly.Data[] = []
    if (data.length) {
        const ids = data.map(r => r.path)
        const labels = data.map(({path}) => root == path ? root : basename(path))
        const values = data.map(r => r.size)
        const parents = data.map(r => r.parent)
        rendered = [{ type: "treemap", ids, labels, values, parents, branchvalues: 'total', }]
        console.log(rendered)
    }
    const pad = 20
    return <>
        <div>
            <span className="db-url">URL: {url}</span>
            <span className="db-path">
                <Link to={{ pathname: '/', search: `?search=${viewRoot}`, }}>
                    Path: {viewRoot}
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
                        setViewRoot(id)
                    } catch (error) {
                        console.error(error)
                    }
                }
            }
        />
    </>
}
