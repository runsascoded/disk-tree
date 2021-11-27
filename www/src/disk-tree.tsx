import {useLocation} from "react-router-dom";
import {build} from "./query";
import React, {useEffect, useState} from "react";
import {Row} from "./data";
import {Worker} from "./worker";
import Plot from "react-plotly.js";

const { fromEntries } = Object

import styled from "styled-components";

const Styled = styled.div`
    .js-plotly-plot {
        width 100%;
        height: 600px;
    }
    .user-svg-container {
        height: 600px;
    }
`

export function DiskTree({ url, worker }: { url: string, worker: Worker }) {
    const [ data, setData ] = useState<Row[]>([])
    const [ limit, setLimit ] = useState(100)
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
            console.log("missingParents:", missingParents)

        },
        [ data, ]
    )

    let rendered: Plotly.Data[] = []
    if (data.length) {
        const labels = data.map(r => r.path)
        const values = data.map(r => r.size)
        const parents = data.map(r => r.parent)
        rendered = [{ type: "treemap", labels, values, parents, branchvalues: 'total', }]
        console.log(rendered)
    }
    return <Styled>
        <span>URL: {url}</span>
        <Plot data={rendered} layout={{ autosize: true }} />
    </Styled>
}
