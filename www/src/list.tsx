import React, {useEffect, useMemo, useState} from "react";
import {Row} from "./data";
import {Table} from "./table";
import {basename, renderSize} from "./utils";
import {Column} from "react-table";
import moment from "moment";
import {Link, useLocation, useParams} from "react-router-dom";
import {Styles} from "./styles";
import {Worker} from './worker';
import {Filter, Sort} from "./query";

const { ceil, } = Math

export function List({ url, worker }: { url: string, worker: Worker }) {
    const { testvalue } = useParams();
    const { pathname, search: query, hash } = useLocation();
    const querySearch = new URLSearchParams(query).get('search')
    console.log("location:", pathname, query, hash, querySearch)

    const [ data, setData ] = useState<Row[]>([])
    const [ datetimeFmt, setDatetimeFmt ] = useState('YYYY-MM-DD HH:mm:ss')
    const [ rowCount, setRowCount ] = useState<number | null>(null)
    const [ pageCount, setPageCount ] = useState(0)
    const [ sorts, setSorts ] = useState<Sort[]>([])
    const [ filters, setFilters ] = useState<Filter[]>([])
    console.log("sorts:", sorts)
    const [ searchValue, setSearchValue ] = useState('')
    const [ searchPrefix, setSearchPrefix ] = useState(false)
    const [ searchSuffix, setSearchSuffix ] = useState(false)
    const [ hasSearched, setHasSearched ] = useState(false)
    // const [ b64State, setB64State ] = useState('')

    const search = { value: searchValue, prefix: searchPrefix, suffix: searchSuffix, }
    const searchFields = [ searchValue, searchPrefix, searchSuffix ]

    const initialPageSize = 10

    console.log("hash:", window.location.hash)

    useEffect(
        () => {
            if (querySearch) {
                console.log("setting search path:", querySearch)
                setSearchValue(querySearch)
            }
        },
        [ querySearch, ]
    )

    // useEffect(
    //     () => {
    //         const json = JSON.stringify({ sorts, filters })
    //         const encoded = encode(json)
    //         console.log("encoded:", json, encoded, encoded.length)
    //         setB64State(encoded)
    //     },
    //     [ sorts, filters, ]
    // )

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
            worker.count({ table: 'file', filters }).then(setRowCount)
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
            if (searchValue) {
                setHasSearched(true)
            }
        },
        searchFields
    )

    // search -> filters
    useEffect(
        () => {
            if (!search?.value && !hasSearched) return
            const newFilter = {
                column: 'path',
                value: search?.value || '',
                prefix: search?.prefix || false,
                suffix: search?.suffix || false,
            }
            let { filter, rest } =
                filters.reduce<{ filter?: Filter, rest: Filter[] }>(
                    ({ filter, rest }, cur) =>
                        cur.column === 'path' ?
                            { filter: newFilter, rest } :
                            { filter, rest: rest.concat([cur]) },
                    { rest: [] },
                )
            if (!filter) {
                filter = newFilter
            }
            const newFilters: Filter[] = [ filter ].concat(rest)
            console.log("newFilters:", newFilters)
            setFilters(newFilters)
        },
        searchFields,
    )

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

    const columnProps: { [id: string]: object } = {
        'kind': {
            style: { textAlign: 'center', },
        },
    }
    const getColumnProps = (column: Column<Row>): object =>
        column.id &&
        columnProps[column.id] ||
        { style: { textAlign: 'right', }}

    const handleHeaderClick = (column: string) => {
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
    }

    const handleCellClick = (column: string, value: string) => {
        console.log("search:", column, value)
        if (column == 'parent' || column == 'path') {
            setSearchValue(value)
        }
    }

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
                            value={searchValue}
                            onChange={e => setSearchValue(e.target.value)}
                        />
                        <span className="input-group-append">
                            <button
                                className="btn btn-outline-secondary border-left-0 border"
                                type="button"
                                onClick={() => setSearchValue('')}
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
                setData={setData}
                pageCount={pageCount}
                updatePageCount={setPageCount}
                rowCount={rowCount}
                handleHeaderClick={handleHeaderClick}
                handleCellClick={handleCellClick}
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
