import React, {MouseEvent, useEffect, useMemo, useState} from "react";
import {Row} from "./data";
import {Table} from "./table";
import {basename, Setter, stopPropagation} from "./utils";
import {Column, IdType} from "react-table";
import moment from "moment";
import {Link} from "react-router-dom";
import {Styles} from "./styles";
import {Worker} from './worker';
import {Filter} from "./query";
import {getQueryString, stringQueryState, toQueryString, useQueryState} from "./search-params";
import {Sort, sortsQueryState} from "./sorts";
import {styled, ThemeProvider, Tooltip, tooltipClasses, TooltipProps} from "@mui/material"
import {computeSize, SizeFmt} from "./size";
import {Radios} from "./radios";
import theme from "./theme";
import createPersistedState from 'use-persisted-state';

const { ceil, round, } = Math
const { assign, entries } = Object

function TreeLinkCell(path: string, ...classes: string[]): JSX.Element
function TreeLinkCell([path, display]: [string, string], ...classes: string[]): JSX.Element
function TreeLinkCell(arg: string | [string, string], ...classes: string[]) {
    const searchParams = new URLSearchParams()
    const [ path, display ] = typeof arg === 'string' ? [ arg, arg ] : arg
    searchParams.set('path', path)
    const queryString = getQueryString({ searchParams, })
    return (
        <span className={["cell-span"].concat(classes).join(' ')}>
            <Link to={{ pathname: 'disk-tree', search: queryString }} >
                <span className="disk-tree-icon">🌲</span>
            </Link>
            {display}
        </span>
    )
}

function SizeCell(size: number, sizeFmt: SizeFmt) {
    const { num: n, suffix } = computeSize(size, sizeFmt)
    const rendered = n >= 100 ? round(n).toString() : n.toFixed(1)
    return <span className="cell-size">
        <span className="cell-size-num">{rendered}</span>
        { suffix && <span className="cell-size-suffix">{suffix}</span> }
    </span>
}

type HeaderSettings<T extends string> = {
    choices: { label: string, name: T }[]
    choice: T | null
    setChoice: Setter<T>
}

function ColumnHeader<T extends string>(
    label: string,
    headerSettings?: HeaderSettings<T>,
) {
    return function(r: { column: Column<Row> }) {
        const body: JSX.Element =
            headerSettings ?
                <Radios {...headerSettings} /> :
                <div>no choices available</div>

        return (
            <ColumnHeaderTooltip arrow placement="bottom-start" title={
                <div className="settings-tooltip" onClick={stopPropagation}>
                    {body}
                </div>
            }>
                <span className="header-span">
                    <span className="settings-icon">⚙️</span>
                    {label}
                </span>
            </ColumnHeaderTooltip>
        )
    }
}

const ColumnHeaderTooltip = styled(
    ({ className, ...props }: TooltipProps) => <Tooltip {...props} classes={{ popper: className }} />
)(({ theme }) => ({
    [`& .${tooltipClasses.tooltip}`]: {
        backgroundColor: theme.palette.common.black, //'#313131',
        color: theme.palette.common.white,
        // backgroundColor: theme.palette.common.white,
        // color: 'rgba(0, 0, 0, 0.87)',
        // boxShadow: theme.shadows[1],
        // fontSize: 11,
    },
}));

type DatetimeFmt = 'YYYY-MM-DD HH:mm:ss' | 'relative'

moment.locale('en', {
    relativeTime: {
        future: 'in %s',
        past: '%s ago',
        s:  'seconds',
        ss: '%ss',
        m:  'a minute',
        mm: '%dm',
        h:  'an hour',
        hh: '%dh',
        d:  'a day',
        dd: '%dd',
        M:  'a month',
        MM: '%dM',
        y:  'a year',
        yy: '%dY'
    }
});

const useSizeFmt = createPersistedState('sizeFmt')
const useDatetimeFmt = createPersistedState('datetimeFmt')

const defaultWidths = {
    kind: 50,
    parent: 400,
    path: 300,
    size: 120,
    mtime: 200,
    num_descendants: 120,
    checked_at: 200.
}
const useColumnWidths = createPersistedState('column-widths')

export function List({ url, worker }: { url: string, worker: Worker }) {
    const [ data, setData ] = useState<Row[]>([])
    const [ datetimeFmt, setDatetimeFmt ] = useDatetimeFmt<DatetimeFmt>('YYYY-MM-DD HH:mm:ss')
    const [ rowCount, setRowCount ] = useState<number | null>(null)
    const [ pageCount, setPageCount ] = useState(0)
    const [ filters, setFilters ] = useState<Filter[]>([])
    const [ searchValue, setSearchValue, ] = useQueryState('search', stringQueryState)
    const [ columnWidths, setColumnWidths ] = useColumnWidths<{ [k: string]: number }>(defaultWidths)

    const [ sizeFmt, setSizeFmt ] = useSizeFmt<SizeFmt>('iec')

    const [ searchPrefix, setSearchPrefix ] = useState(false)
    const [ searchSuffix, setSearchSuffix ] = useState(false)

    const search = { value: searchValue, prefix: searchPrefix, suffix: searchSuffix, }
    const searchFields = [ searchValue, searchPrefix, searchSuffix ]

    const initialPageSize = 10

    const [ sorts, setSorts, ] = useQueryState<Sort[]>('sort', sortsQueryState)

    // console.log("sorts:", sorts, "filters:", filters)

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

    const sizeHeaderSettings: HeaderSettings<SizeFmt> = {
        choices: [
            { name: 'iec', label: 'Human Readable (IEC)', },
            { name: 'iso', label: 'Human Readable (ISO)', },
            { name: 'bytes', label: 'Bytes', },
        ],
        choice: sizeFmt,
        setChoice: setSizeFmt,
    }

    const datetimeHeaderSettings: HeaderSettings<DatetimeFmt> = {
        choices: [
            { name: 'relative', label: 'Relative', },
            { name: 'YYYY-MM-DD HH:mm:ss', label: 'YYYY-MM-DD HH:mm:ss', },
        ],
        choice: datetimeFmt,
        setChoice: setDatetimeFmt,
    }

    function renderDatetime(date: Date, fmt: DatetimeFmt): string {
        const m = moment(date)
        if (fmt == 'relative') {
            return m.fromNow(true)
        } else {
            return m.format(fmt)
        }
    }

    const columns: Column<Row>[] = useMemo(
        () => {
            const columns: Column<Row>[] = [
                { id: 'kind', Header: ColumnHeader('Kind'), accessor: r => r.kind == 'dir' ? '📂' : '💾' },
                { id: 'parent', Header: ColumnHeader('Parent'), accessor: r => TreeLinkCell(r.parent, 'cell-parent'), },
                { id: 'path', Header: ColumnHeader('Name'), accessor: r => TreeLinkCell([r.path, basename(r.path)], 'cell-path'), },
                { id: 'size', Header: ColumnHeader('Size', sizeHeaderSettings), accessor: r => SizeCell(r.size, sizeFmt), },
                { id: 'mtime', Header: ColumnHeader('Modified', datetimeHeaderSettings), accessor: r => renderDatetime(r.mtime, datetimeFmt), },
                { id: 'num_descendants', Header: 'Descendants', accessor: 'num_descendants', },
                { id: 'checked_at', Header: ColumnHeader('Checked At', datetimeHeaderSettings), accessor: r => renderDatetime(r.checked_at, datetimeFmt), },
            ]
            return columns.map(
                c => {
                    if (c.id && c.id in columnWidths) {
                        c.width = columnWidths[c.id]
                    }
                    return c
                }
            )
        },
        [ sizeFmt, datetimeFmt, sizeHeaderSettings, , datetimeHeaderSettings, ]
    )

    function onColumnResize(newColumnWidths: { [k: string]: number }) {
        const nxtColumnWidths = assign({}, columnWidths)
        entries(newColumnWidths).map(([ column, width ]: [ string, number ]) => {
            nxtColumnWidths[column] = width
        })
        console.log("column widths:", columnWidths, nxtColumnWidths)
        setColumnWidths(nxtColumnWidths)
    }

    const defaultColumnProps = { style: { textAlign: 'right', }}
    const columnProps: { [id: string]: object } = {
        'kind': {
            style: { textAlign: 'center', },
        },
    }
    const getColumnProps = (column: Column<Row>): object =>
        assign(
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
            <ThemeProvider theme={theme}>
            <div className="list-page header-controls row no-gutters search">
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
                onColumnResize={onColumnResize}
            />
            </ThemeProvider>
        </Styles>
    )
}
