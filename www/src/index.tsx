import { createDbWorker } from "sql.js-httpvfs";
import React, { useEffect, useState, } from 'react';
import ReactDOM from 'react-dom'
import styled from 'styled-components'
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


import { Column, useTable, usePagination, useSortBy, } from 'react-table'
import {WorkerHttpvfs} from "sql.js-httpvfs/dist/db";
import {Sort, Table} from "./table";

const Styles = styled.div`
  padding: 1rem;

  table {
    border-spacing: 0;
    border: 1px solid black;

    tr {
      :last-child {
        td {
          border-bottom: 0;
        }
      }
    }

    th,
    td {
      margin: 0;
      padding: 0.5rem;
      border-bottom: 1px solid black;
      border-right: 1px solid black;

      :last-child {
        border-right: 0;
      }
    }
  }

  .pagination {
    padding: 0.5rem;
  }
`

async function fetchPage(
    worker: WorkerHttpvfs,
    pageIndex: number,
    pageSize: number,
    sort?: Sort,
): Promise<Row[]> {
    let query = `SELECT * FROM file`
    if (sort) {
        query += ` ORDER BY ${sort.column}`
        if (sort.desc) {
            query += ` DESC`
        }
    }
    query += ` LIMIT ${pageSize} OFFSET ${pageIndex * pageSize}`
    console.log("query:", query)
    return worker.db.query(query).then((rows) => {
        console.log(`Page ${pageIndex}x${pageSize}:`, rows);
        return rows as Row[]
    })
}

function App1() {
    const url: string = "/assets/disk-tree1k.db";
    const [ rowCount, setRowCount ] = React.useState<number | null>(null)
    const [ data, setData ] = useState<Row[]>([])
    const [ loadingWorker, setLoadingWorker ] = React.useState(false);
    const [ loadingRowCount, setLoadingRowCount ] = React.useState(false)
    const [ pageCount, setPageCount ] = React.useState(0)
    const [ loadingData, setLoadingData ] = React.useState(false);
    const [ sort, setSort ] = React.useState<Sort | undefined>()
    console.log("sort:", sort)
    const [ worker, setWorker ] = React.useState<WorkerHttpvfs | null>(null)

    useEffect(
        () => {
            console.log("effect; loadingWorker:", loadingWorker)
            async function initWorker() {
                console.log("initWorker; loading:", loadingWorker)
                if (loadingWorker) {
                    console.log("skipping init, loading already true")
                    return
                }
                try {
                    setLoadingWorker(true);
                    console.log("Fetching DB…", url);
                    const worker: WorkerHttpvfs = await createDbWorker(
                        [
                            {
                                from: "inline",
                                config: {
                                    serverMode: "full",
                                    url: url,
                                    //requestChunkSize: 4096,
                                    requestChunkSize: 8 * 1048576,
                                },
                            },
                        ],
                        workerUrl.toString(),
                        wasmUrl.toString()
                    );
                    console.log("setting worker")
                    setWorker(worker)
                } catch (error) {
                    setLoadingWorker(false);
                }
            }

            if (url !== "") {
                initWorker();
            }
        },
        [url]
    );

    useEffect(
        () => {
            async function initRowCount(worker: WorkerHttpvfs) {
                if (loadingRowCount) {
                    console.log("page count: already loading")
                } else {
                    console.log("page count, loading")
                    setLoadingRowCount(true)
                    console.log("setLoadingRowCount true")
                    const [{rowCount}] = (await worker.db.query(`SELECT count(*) AS rowCount FROM file`)) as { rowCount: number }[]
                    console.log(`${rowCount} rows`)
                    setLoadingRowCount(false)
                    console.log("setLoadingRowCount false, rowCount:", rowCount)
                    setRowCount(rowCount)
                }
            }

            if (worker !== null) {
                initRowCount(worker)
            } else {
                console.log("page count: worker is null")
            }
        },
        [ worker ],
    )

    const initialPageSize = 20

    useEffect(
        () => {
            if (rowCount !== null) {
                const pageCount = Math.ceil(rowCount / initialPageSize)
                console.log("update pageCount from rowCount:", rowCount, pageCount)
                setPageCount(pageCount)
            } else {
                console.log("update pageCount from rowCount:", rowCount)
            }
        },
        [ rowCount, ]
    )

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

    const fetchData = React.useCallback(
        ({ worker, pageSize, pageIndex }: {
            worker: WorkerHttpvfs,
            pageSize: number,
            pageIndex: number,
            sort?: Sort,
        }) => {
        // Set the loading state
        console.log(`fetching page ${pageIndex}`)

        if (worker !== null) {
            if (!loadingData) {
                console.log("fetching")
                setLoadingData(true)
                console.log("setLoadingData(true)")
                fetchPage(worker, pageIndex, pageSize, sort).then((rows) => {
                    console.log("fetched page:", rows)
                    setLoadingData(false)
                    console.log("setLoadingData(false)")
                    setData(rows)
                })
            } else {
                console.log("skipping fetch, loadingData != false:", loadingData)
            }
        } else {
            console.log("worker === null")
        }
    }, [ worker, rowCount, sort, ])

    return (
        <Styles>
        <Table
            columns={columns}
            data={data}
            fetchData={fetchData}
            pageCount={pageCount}
            updatePageCount={setPageCount}
            rowCount={rowCount}
            handleHeaderClick={(column) => {
                console.log("header click:", column)
                if (sort?.column == column) {
                    console.log("reversing", column)
                    setSort({ column, desc: !sort.desc })
                } else {
                    console.log("setting sort col:", column)
                    setSort({ column, desc: true })
                }
            }}
            sort={sort}
            worker={worker}
            initialPageSize={initialPageSize}
        />
        </Styles>
    )
}

$(document).ready(function () {
    ReactDOM.render(<App1 />, document.getElementById('root'));
});
