import { createDbWorker } from "sql.js-httpvfs";
import React, {useEffect, useState} from 'react';
import ReactDOM from 'react-dom'
// import { Component, createElement } from "react";
// import Plot from 'react-plotly.js';
import $ from 'jquery';
// import {Shape} from "plotly.js";
// import * as Plotly from "plotly.js";

// const { entries, values, keys, fromEntries } = Object
// const Arr = Array.from


const workerUrl = new URL(
    "sql.js-httpvfs/dist/sqlite.worker.js",
    import.meta.url
);
const wasmUrl = new URL("sql.js-httpvfs/dist/sql-wasm.wasm", import.meta.url);

type Row = {
    path: string
    parent: string
    mtime: Date
    num_descendants: number
    size: number
    checked_at: Date
    kind: string
}


import {Column, useTable} from 'react-table'

function useAsyncHook(url: string): [ Row[], string ] {
    const [ data, setData ] = useState<Row[]>([])
    const [loading, setLoading] = React.useState("false");

    React.useEffect(
        () => {
            async function fetchDB() {
                try {
                    setLoading("true");
                    console.log("Fetching DB…", url);
                    const worker = await createDbWorker(
                        [
                            {
                                from: "inline",
                                config: {
                                    serverMode: "full",
                                    url: url,
                                    requestChunkSize: 4096,
                                },
                            },
                        ],
                        workerUrl.toString(),
                        wasmUrl.toString()
                    );
                    const rows = (await worker.db.query(`select * from file limit 20`)) as Row[];
                    console.log("Fetched db:", url);
                    console.log("data:", rows);
                    setData(rows)
                } catch (error) {
                    setLoading("null");
                }
            }

            if (url !== "") {
                fetchDB();
            }
        },
        [url]
    );

    return [data, loading];
}

function App1() {
    const url = "/assets/disk-tree.db";
    const [data, loading] = useAsyncHook(url);

    const columns: Column<Row>[] = React.useMemo(
        () => [
            { Header: 'Path', accessor: 'path', },
            { Header: 'Kind', accessor: 'kind', },
            { Header: 'Size', accessor: 'size', },
            { Header: 'Parent', accessor: 'parent', },
            { Header: 'Modified', accessor: 'mtime', },
            { Header: 'Num Descendants', accessor: 'num_descendants', },
            { Header: 'Checked At', accessor: 'checked_at', },
        ],
        []
    )

    const {
        getTableProps,
        getTableBodyProps,
        headerGroups,
        rows,
        prepareRow,
    } = useTable({ columns, data })

    return (
        <table {...getTableProps()} style={{ border: 'solid 1px blue' }}>
            <thead>
            {headerGroups.map(headerGroup => (
                <tr {...headerGroup.getHeaderGroupProps()}>
                    {headerGroup.headers.map(column => (
                        <th
                            {...column.getHeaderProps()}
                            style={{
                                borderBottom: 'solid 3px red',
                                background: 'aliceblue',
                                color: 'black',
                                fontWeight: 'bold',
                            }}
                        >
                            {column.render('Header')}
                        </th>
                    ))}
                </tr>
            ))}
            </thead>
            <tbody {...getTableBodyProps()}>
            {rows.map(row => {
                prepareRow(row)
                return (
                    <tr {...row.getRowProps()}>
                        {row.cells.map(cell => {
                            return (
                                <td
                                    {...cell.getCellProps()}
                                    style={{
                                        padding: '10px',
                                        border: 'solid 1px gray',
                                        background: 'papayawhip',
                                    }}
                                >
                                    {cell.render('Cell')}
                                </td>
                            )
                        })}
                    </tr>
                )
            })}
            </tbody>
        </table>
    )
}

$(document).ready(function () {
    ReactDOM.render(<App1 />, document.getElementById('root'));
});
