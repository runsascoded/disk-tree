import React, {useEffect, useMemo, useState} from "react";
import {Row} from "./data";
import {Table} from "./table";
import {basename, renderSize, Setter} from "./utils";
import {Column} from "react-table";
import moment from "moment";
import {Link, useNavigate, useLocation, useParams, Navigator, NavigateFunction} from "react-router-dom";
import { createBrowserHistory } from "history";
import {Styles} from "./styles";
import {Worker} from './worker';
import {Filter, Sort} from "./query";
import _ from 'lodash';

const { ceil, } = Math

const sortRegex = /(?<column>[^!\-]+)(?<dir>[!\-]?)/g
const DefaultSorts = [ { column: 'size', desc: true, }]

const parseQuerySorts = function(str: string): Sort[] {
    if (!str) return []
    return (
        Array.from(str.matchAll(sortRegex))
            .map(a =>
                a.groups as { column: string, dir: string }
            )
            .map(
                ( { column, dir }) => {
                    return {
                        column,
                        desc: dir == '-',
                    }
                }
            )
    )
}

const renderQuerySorts = function(sorts: Sort[] | null): string {
    return (sorts || [])
        .map(( { column, desc, }, idx) => {
            // return column + (desc ? '-' : (idx + 1 < sorts.length ? '!' : ''))
            let s = column
            if (desc) s += '-'
            else if (idx + 1 < (sorts?.length || 0)) s += '!'
            return s
        })
        .join('')
}

function queryParamToState<T>(
    { queryKey, queryValue, state, setState, defaultValue, parse }: {
        queryKey: string,
        queryValue: string | null,
        state: T | null,
        setState: Setter <T | null>,
        defaultValue: T,
        parse?: (queryParam: string) => T,
    }
) {
    useEffect(
        () => {
            if (state === null) {
                if (queryValue) {
                    const parsedState = parse ? parse(queryValue) : (queryValue as any as T)
                    console.log(`queryKey ${queryKey} = ${queryValue}: parsed`, parsedState)
                    setState(parsedState)
                } else {
                    console.log(`queryKey ${queryKey} = ${queryValue}: setting default value`, defaultValue)
                    setState(defaultValue)
                }
            }
        },
        [ queryValue ]
    )
}

function stateToQueryParam<T>(
    { queryKey, state, searchParams, navigate, defaultValue, render, eq, replaceChars, }: {
        queryKey: string,
        queryValue: string | null,
        state: T | null,
        searchParams: URLSearchParams,
        navigate: NavigateFunction,
        defaultValue: T,
        render?: (value: T | null) => string,
        eq?: (l: T, r: T) => boolean,
        replaceChars?: { [k: string]: string, },
    }
) {
    useEffect(
        () => {
            if (state === null) return
            if (
                eq ?
                    eq(state, defaultValue) :
                    typeof state === 'object' ?
                        _.isEqual(state, defaultValue) :
                        (state == defaultValue)
            ) {
                searchParams.delete(queryKey)
            } else {
                const queryValue = render ? render(state) : (state as any).toString()
                searchParams.set(queryKey, queryValue)
            }
            let queryString = searchParams.toString()
            replaceChars = replaceChars || { '%2F': '/', '%21': '!', }
            Object.entries(replaceChars).forEach(([ k, v ]) => {
                queryString = queryString.replaceAll(k, v)
            })
            console.log(`queryKey ${queryKey} new string`, queryString)
            navigate(
                {
                    pathname: "",
                    search: queryString,
                },
                { replace: true, },
            )
        },
        [ state ]
    )
}

export function List({ url, worker }: { url: string, worker: Worker }) {
    const { pathname, search: query, hash } = useLocation();
    const searchParams = new URLSearchParams(query)
    const querySearch = searchParams.get('search')
    const querySort = searchParams.get('sort')
    console.log("render! location:", pathname, query, hash, querySearch)

    let navigate = useNavigate()

    const [ data, setData ] = useState<Row[]>([])
    const [ datetimeFmt, setDatetimeFmt ] = useState('YYYY-MM-DD HH:mm:ss')
    const [ rowCount, setRowCount ] = useState<number | null>(null)
    const [ pageCount, setPageCount ] = useState(0)
    const [ sorts, setSorts ] = useState<Sort[] | null>(null)
    const [ filters, setFilters ] = useState<Filter[]>([])
    const [ searchValue, setSearchValue ] = useState<string | null>(null)
    const [ searchPrefix, setSearchPrefix ] = useState(false)
    const [ searchSuffix, setSearchSuffix ] = useState(false)
    // const [ hasSearched, setHasSearched ] = useState(false)
    // const [ b64State, setB64State ] = useState('')

    const search = { value: searchValue, prefix: searchPrefix, suffix: searchSuffix, }
    const searchFields = [ searchValue, searchPrefix, searchSuffix ]

    const initialPageSize = 10

    // `?search` query param -> searchValue
    queryParamToState({
        queryKey: 'search',
        queryValue: querySearch,
        state: searchValue,
        setState: setSearchValue,
        defaultValue: "",
    })

    // searchValue -> `?search` query param
    stateToQueryParam<string>({
        queryKey: 'search',
        queryValue: querySearch,
        state: searchValue,
        defaultValue: "",
        searchParams,
        navigate,
    })
    // useEffect(
    //     () => {
    //         if (!searchValue && !hasSearched) return
    //         console.log("updating search query:", searchValue)
    //         if (searchValue === undefined) {
    //             searchParams.delete('search')
    //         } else {
    //             searchParams.set('search', searchValue)
    //         }
    //         const queryString = searchParams.toString().replaceAll('%2F', '/')
    //         navigate(
    //             {
    //                 pathname: "",
    //                 search: queryString,
    //             },
    //             { replace: true },
    //         )
    //     },
    //     [ searchValue, ]
    // )

    // `?sort` query param -> sorts
    queryParamToState<Sort[] | null>({
        queryKey: 'sort',
        queryValue: querySort,
        state: sorts,
        setState: setSorts,
        defaultValue: DefaultSorts,
        parse: parseQuerySorts,
    })

    // sorts -> `?sort` query param
    stateToQueryParam<Sort[] | null>({
        queryKey: 'sort',
        queryValue: querySort,
        state: sorts,
        defaultValue: DefaultSorts,
        render: renderQuerySorts,
        searchParams,
        navigate,
    })
    // useEffect(
    //     () => {
    //         if (sorts === null) return
    //         if (_.isEqual(sorts, DefaultSorts)) {
    //             searchParams.delete('sort')
    //         } else {
    //             const sortsValue = renderQuerySorts(sorts)
    //             searchParams.set('sort', sortsValue)
    //         }
    //         const queryString = searchParams.toString().replaceAll('%2F', '/')
    //         console.log("new queryString:", queryString)
    //         navigate(
    //             {
    //                 pathname: "",
    //                 search: queryString,
    //             },
    //             { replace: true, },
    //         )
    //     },
    //     [ sorts ]
    // )

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
                // console.log("update pageCount from rowCount:", rowCount, pageCount)
                setPageCount(pageCount)
            } else {
                // console.log("update pageCount from rowCount:", rowCount)
            }
        },
        [ rowCount, ]
    )

    // useEffect(
    //     () => {
    //         if (searchValue) {
    //             setHasSearched(true)
    //         }
    //     },
    //     searchFields
    // )

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
            // console.log("newFilters:", newFilters)
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
        // console.log("header click:", column)
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
        // console.log("newSorts:", newSorts)
        setSorts(newSorts)
    }

    const handleCellClick = (column: string, value: string) => {
        // console.log("search:", column, value)
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
            <Link to={{ pathname: "disk-tree", search: querySearch ? `?path=${querySearch}` : undefined }}>Disk Tree</Link>
        </Styles>
    )
}
