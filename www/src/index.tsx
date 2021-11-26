import { createDbWorker } from "sql.js-httpvfs";
import React, { useEffect, useState, useMemo, useCallback, } from 'react';
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
import {Filter, Sort, Table} from "./table";
import {build} from "./query";

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

  .table {
    display: inline-block;
    border-spacing: 0;
    border: 1px solid black;
    width: auto;

    .tr {
      :last-child {
        .td {
          border-bottom: 0;
        }
      }
    }

    .th {
      cursor: pointer;
    }

    .th,
    .td {
      margin: 0;
      padding: 0.5rem;
      border-bottom: 1px solid black;
      border-right: 1px solid black;

      overflow-wrap: anywhere;

      ${'' /* In this example we use an absolutely position resizer,
       so this is required. */}
      position: relative;

      :last-child {
        border-right: 0;
      }

      .resizer {
        display: inline-block;
        background: transparent;
        width: 8px;
        height: 100%;
        position: absolute;
        right: 0;
        top: 0;
        transform: translateX(50%);
        z-index: 1;
        ${'' /* prevents from scrolling while dragging on touch devices */}
        touch-action:none;

        &.isResizing {
          background: red;
        }
      }
    }
  }
`

async function fetchPage(
    worker: WorkerHttpvfs,
    pageIndex: number,
    pageSize: number,
    sorts: Sort[],
    filters: Filter[],
): Promise<Row[]> {
    const query = build(
        {
            table: 'file',
            limit: pageSize,
            offset: pageSize * pageIndex,
            count: undefined,
            sorts,
            filters,
        }
    )
    console.log("query:", query)
    return worker.db.query(query).then((rows) => {
        console.log(`Page ${pageIndex}x${pageSize}:`, rows);
        return rows as Row[]
    })
}

function App() {
    const url: string = "/assets/disk-tree1k.db";
    const [ rowCount, setRowCount ] = useState<number | null>(null)
    const [ data, setData ] = useState<Row[]>([])
    const [ loadingWorker, setLoadingWorker ] = useState(false);
    const [ loadingRowCount, setLoadingRowCount ] = useState(false)
    const [ pageCount, setPageCount ] = useState(0)
    const [ loadingData, setLoadingData ] = useState(false);
    const [ sorts, setSorts ] = useState<Sort[]>([])
    const [ filters, setFilters ] = useState<Filter[]>([])
    console.log("sorts:", sorts)
    const [ worker, setWorker ] = useState<WorkerHttpvfs | null>(null)
    const [ search, setSearch ] = useState("")
    const [ hasSearched, setHasSearched ] = useState(false)

    const initialPageSize = 20

    // URL -> Worker
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

    // (Worker, Filters) -> RowCount
    useEffect(
        () => {
            async function initRowCount(worker: WorkerHttpvfs) {
                if (loadingRowCount) {
                    console.log("page count: already loading")
                } else {
                    console.log("page count, loading")
                    setLoadingRowCount(true)
                    console.log("setLoadingRowCount true")
                    const query = build({
                        table: 'file',
                        count: 'rowCount',
                        filters,
                    })
                    console.log("query:", query)
                    const [{rowCount}] = await worker.db.query(query) as { rowCount: number }[]
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
        [ worker, filters ],
    )

    // RowCount -> PageCount
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

    useEffect(
        () => {
            if (search) {
                setHasSearched(true)
            }
        },
        [ search ]
    )

    // search -> filters
    useEffect(
        () => {
            if (!search && !hasSearched) return
            let { filter, rest } =
                filters.reduce<{ filter?: Filter, rest: Filter[] }>(
                    ({ filter, rest }, {column, value}) =>
                        column === 'path' ?
                            { filter: { column, value: search }, rest } :
                            { filter, rest: rest.concat([{ column, value }]) },
                    { rest: [] },
                )
            if (!filter) {
                filter = { column: 'path', value: search, }
            }
            const newFilters: Filter[] = [ filter ].concat(rest)
            console.log("newFilters:", newFilters)
            setFilters(newFilters)
        },
        [ search, ],
    )

    const columns: Column<Row>[] = useMemo(
        () => [
            { Header: 'Path', accessor: 'path', },
            { Header: 'Kind', accessor: 'kind', },
            { Header: 'Size', accessor: 'size', },
            { Header: 'Parent', accessor: 'parent', },
            { Header: 'Modified', accessor: 'mtime', },
            { Header: 'Descendants', accessor: 'num_descendants', },
            { Header: 'Checked At', accessor: 'checked_at', },
        ],
        []
    )

    const fetchData = useCallback(
        ({ worker, pageSize, pageIndex, sorts, filters, }: {
            worker: WorkerHttpvfs,
            pageSize: number,
            pageIndex: number,
            sorts: Sort[],
            filters: Filter[],
        }) => {
        // Set the loading state
        console.log(`fetching page ${pageIndex}`)

        if (worker !== null) {
            if (!loadingData) {
                console.log("fetching")
                setLoadingData(true)
                console.log("setLoadingData(true)")
                fetchPage(worker, pageIndex, pageSize, sorts, filters, ).then((rows) => {
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
    }, [ worker, rowCount, sorts, filters, ])

    return (
        <Styles>
            <div className="row no-gutters search">
                <label>Search:</label>
                <input
                    type="text"
                    placeholder=""
                    value={search}
                    onChange={e => setSearch(e.target.value)}
                />
            </div>
            <Table
                columns={columns}
                data={data}
                fetchData={fetchData}
                pageCount={pageCount}
                updatePageCount={setPageCount}
                rowCount={rowCount}
                handleHeaderClick={column => {
                    console.log("header click:", column)
                    const sort = sorts.find(({column: col}) => col == column)
                    const desc = sort?.desc
                    let newSorts: Sort[] =
                        (desc === true) ?
                            [] :
                            (desc === false) ?
                                [{ column, desc: true }] :
                                [{ column, desc: false }]
                    const rest = sorts.filter(({column: col}) => col != column)
                    newSorts = newSorts.concat(rest)
                    console.log("newSorts:", newSorts)
                    setSorts(newSorts)
                }}
                handleCellClick={
                    (column, value) => {
                        console.log("search:", column, value)
                        if (column == 'parent' || column == 'path') {
                            setSearch(value)
                        }
                    }
                }
                sorts={sorts}
                filters={filters}
                worker={worker}
                initialPageSize={initialPageSize}
            />
        </Styles>
    )
}

$(document).ready(function () {
    ReactDOM.render(<App/>, document.getElementById('root'));
});
