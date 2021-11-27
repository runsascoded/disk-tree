import {WorkerHttpvfs} from "sql.js-httpvfs/dist/db";
import React, {useCallback, useEffect, useMemo, useState} from "react";
import {Row} from "./data";
import {Filter, Sort, Table} from "./table";
import {basename, encode, renderSize} from "./utils";
import {build} from "./query";
import {Column} from "react-table";
import moment from "moment";
import {Link} from "react-router-dom";
import {Styles} from "./styles";

const { ceil, } = Math

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

export function List({ url, worker }: { url: string, worker: WorkerHttpvfs | null }) {
    const [ data, setData ] = useState<Row[]>([])
    const [ loadingData, setLoadingData ] = useState(false);
    const [ rowCount, setRowCount ] = useState<number | null>(null)
    const [ loadingRowCount, setLoadingRowCount ] = useState(false)
    const [ pageCount, setPageCount ] = useState(0)
    const [ sorts, setSorts ] = useState<Sort[]>([])
    const [ filters, setFilters ] = useState<Filter[]>([])
    console.log("sorts:", sorts)
    const [ search, setSearch ] = useState("")
    const [ hasSearched, setHasSearched ] = useState(false)
    const [ b64State, setB64State ] = useState('')

    const initialPageSize = 10

    console.log("hash:", window.location.hash)

    useEffect(
        () => {
            const json = JSON.stringify({ sorts, filters })
            const encoded = encode(json)
            console.log("encoded:", json, encoded, encoded.length)
            setB64State(encoded)
        },
        [ sorts, filters, ]
    )

    // useEffect(
    //     () => {
    //         console.log("update hash:", b64State)
    //         window.location.hash = b64State
    //     },
    //     [ b64State ]
    // )

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
                const pageCount = ceil(rowCount / initialPageSize)
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

    const datetimeFmt = 'YYYY-MM-DD HH:mm:ss'

    const columns: Column<Row>[] = useMemo(
        () => [
            { id: 'kind', Header: 'Kind', accessor: row => row.kind == 'dir' ? '📂' : '💾', width: 50, },
            { id: 'parent', Header: 'Parent', accessor: 'parent', width: 400, },
            { id: 'path', Header: 'Name', accessor: row => basename(row.path), width: 300, },
            { id: 'size', Header: 'Size', accessor: row => renderSize(row.size), width: 120, },
            { id: 'mtime', Header: 'Modified', accessor: row => moment(row.mtime).format(datetimeFmt), },
            { id: 'num_descendants', Header: 'Descendants', accessor: 'num_descendants', width: 120, },
            { id: 'checked_at', Header: 'Checked At', accessor: row => moment(row.checked_at).format(datetimeFmt), },
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
        }, [ worker, sorts, filters, ])

    const columnProps: { [id: string]: object } = {
        'kind': {
            style: { textAlign: 'center', },
        },
    }
    const getColumnProps = (column: Column<Row>): object =>
        column.id &&
        columnProps[column.id] ||
        { style: { textAlign: 'right', }}

    return (
        <Styles>
            <div className="row no-gutters search">
                <div className="row">
                    <div className="input-group col-md-12">
                        <input
                            className="form-control py-2 border-right-0 border"
                            type="search"
                            placeholder="Search"
                            id="example-search-input"
                            value={search}
                            onChange={e => setSearch(e.target.value)}
                        />
                        <span className="input-group-append">
                            <button
                                className="btn btn-outline-secondary border-left-0 border"
                                type="button"
                                onClick={() => setSearch('')}
                            >
                                <i className="fa fa-times"/>
                            </button>
                        </span>
                    </div>
                </div>
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
                getColumnProps={getColumnProps}
                sorts={sorts}
                filters={filters}
                worker={worker}
                initialPageSize={initialPageSize}
            />
            <Link to="disk-tree#abc=def">Disk Tree</Link>
        </Styles>
    )
}
