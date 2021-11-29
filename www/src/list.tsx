import React, {useEffect, useMemo, useState} from "react";
import {Row} from "./data";
import {Table} from "./table";
import {basename, renderSize} from "./utils";
import {Column} from "react-table";
import moment from "moment";
import {Link} from "react-router-dom";
import {Styles} from "./styles";
import {Worker} from './worker';
import {Filter} from "./query";
import {getQueryString, useQueryState, stringQueryState, toQueryString} from "./search-params";
import {Sort, sortsQueryState} from "./sorts";

const { ceil, } = Math

export function List({ url, worker }: { url: string, worker: Worker }) {
    const [ data, setData ] = useState<Row[]>([])
    const [ datetimeFmt, setDatetimeFmt ] = useState('YYYY-MM-DD HH:mm:ss')
    const [ rowCount, setRowCount ] = useState<number | null>(null)
    const [ pageCount, setPageCount ] = useState(0)
    const [ filters, setFilters ] = useState<Filter[]>([])
    const [ searchValue, setSearchValue, ] = useQueryState('search', stringQueryState)

    const [ searchPrefix, setSearchPrefix ] = useState(false)
    const [ searchSuffix, setSearchSuffix ] = useState(false)

    const search = { value: searchValue, prefix: searchPrefix, suffix: searchSuffix, }
    const searchFields = [ searchValue, searchPrefix, searchSuffix ]

    const initialPageSize = 10

    const [ sorts, setSorts, ] = useQueryState<Sort[]>('sort', sortsQueryState)

    console.log("sorts:", sorts, "filters:", filters)

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
                setPageCount(pageCount)
            }
        },
        [ rowCount, ]
    )

    // search -> filters
    useEffect(
        () => {
            if (search === null) return
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
            setFilters(newFilters)
        },
        searchFields,
    )

    function TreeLinkCell(path: string, display?: string) {
        const searchParams = new URLSearchParams()
        searchParams.set('path', path)
        display = display || path
        const queryString = getQueryString({ searchParams, })
        return (
            <span className="cell-span">
                <Link to={{ pathname: 'disk-tree', search: queryString }} >
                    <span className="disk-tree-icon">🌲</span>
                </Link>
                {display}
            </span>
        )
    }

    const columns: Column<Row>[] = useMemo(
        () => [
            { id: 'kind', Header: 'Kind', accessor: r => r.kind == 'dir' ? '📂' : '💾', width: 50, },
            { id: 'parent', Header: 'Parent', accessor: r => TreeLinkCell(r.parent), width: 400,},
            { id: 'path', Header: 'Name', accessor: r => TreeLinkCell(r.path, basename(r.path)), width: 300, },
            { id: 'size', Header: 'Size', accessor: r => renderSize(r.size), width: 120, },
            { id: 'mtime', Header: 'Modified', accessor: r => moment(r.mtime).format(datetimeFmt), },
            { id: 'num_descendants', Header: 'Descendants', accessor: 'num_descendants', width: 120, },
            { id: 'checked_at', Header: 'Checked At', accessor: r => moment(r.checked_at).format(datetimeFmt), },
        ],
        []
    )

    const defaultColumnProps = { style: { textAlign: 'right', }}
    const columnProps: { [id: string]: object } = {
        'kind': {
            style: { textAlign: 'center', },
        },
    }
    const getColumnProps = (column: Column<Row>): object =>
        Object.assign(
            {},
            defaultColumnProps,
            column.id && columnProps[column.id] ? columnProps[column.id] : {}
        )

    const handleHeaderClick = (column: string) => {
        console.log("header clicked:", column)
        const sort = sorts?.find(({column: col}) => col == column)
        const desc = sort?.desc
        let newSorts: Sort[] =
            (desc === true) ?
                [] :
                (desc === false) ?
                    [{ column, desc: true }] :
                    [{ column, desc: false }]
        const rest = sorts?.filter(({column: col}) => col != column) || []
        newSorts = newSorts.concat(rest)
        setSorts(newSorts)
    }

    const handleCellClick = (column: string, row: Row) => {
        if (column == 'parent' || column == 'path') {
            setSearchValue(row[column])
        }
    }

    return (
        <Styles>
            <div className="header-controls row no-gutters search">
                <div className="row">
                    <div className="input-group col-md-12">
                        <input
                            className="form-control py-2 border-right-0 border"
                            type="search"
                            placeholder="Search"
                            id="search-input"
                            value={searchValue || ''}
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
                        <span className="disk-tree-icon large">
                            <Link to={{
                                pathname: "disk-tree",
                                search:
                                    searchValue &&
                                    toQueryString({ path: searchValue }) ||
                                    undefined
                            }}>
                                🌲
                            </Link>
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
                sorts={sorts || []}
                filters={filters}
                worker={worker}
                initialPageSize={initialPageSize}
            />
        </Styles>
    )
}
