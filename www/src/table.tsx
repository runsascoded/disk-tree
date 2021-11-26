import {Column, useBlockLayout, usePagination, useResizeColumns, useTable} from "react-table";
import {WorkerHttpvfs} from "sql.js-httpvfs/dist/db";
import React, {useEffect} from "react";
import { Setter } from "./utils";

export type Sort = { column: string, desc: boolean, }
export type Filter = { column: string, value: string }

export function Table<Row extends object>(
    {
        columns,
        data,
        fetchData,
        sorts,
        filters,
        handleHeaderClick,
        handleCellClick,
        rowCount,
        pageCount,
        worker,
        updatePageCount,
        initialPageSize,
    }: {
        columns: Column<Row>[],
        data: Row[],
        fetchData: (
            { worker, pageSize, pageIndex, sorts, filters, }: {
                worker: WorkerHttpvfs,
                pageSize: number,
                pageIndex: number,
                sorts: Sort[],
                filters: Filter[],
            }
        ) => void,
        sorts: Sort[],
        filters: Filter[],
        handleHeaderClick: (column: string) => void,
        handleCellClick: (column: string, value: string) => void,
        pageCount: number,
        updatePageCount: Setter<number>,
        rowCount: number | null,
        worker: WorkerHttpvfs | null,
        initialPageSize: number
    }
) {
    const defaultColumn = React.useMemo(
        () => ({
            minWidth: 100,
            width: 200,
            maxWidth: 600,
        }),
        []
    )

    const {
        getTableProps,
        getTableBodyProps,
        headerGroups,
        prepareRow,
        page,
        canPreviousPage,
        canNextPage,
        pageOptions,
        gotoPage,
        nextPage,
        previousPage,
        setPageSize,
        state: { pageIndex, pageSize },
    } = useTable<Row>(
        {
            columns,
            data,
            initialState: { pageIndex: 0, pageSize: initialPageSize, },
            manualPagination: true,
            pageCount,
            defaultColumn,
        },
        useBlockLayout,
        useResizeColumns,
        usePagination,
    )
    // Update data page based on relevant changes
    useEffect(
        () => {
            if (worker !== null) {
                console.log("table fetching")
                fetchData({ worker, pageIndex, pageSize, sorts, filters, })
            } else {
                console.log("null worker")
            }
        },
        [ fetchData, worker, pageIndex, pageSize, sorts, filters, ],
    )

    // (rowCount, pageSize) -> pageIndex
    useEffect(
        () => {
            if (rowCount && pageIndex * pageSize > rowCount) {
                gotoPage(0)
            }
        },
        [ rowCount, pageSize, ]
    )

    // (pageSize, rowCount) -> pageCount
    useEffect(
        () => {
            if (rowCount !== null) {
                const pageCount = Math.ceil(rowCount / pageSize)
                console.log("updatePageCount:", pageCount, `(${rowCount}/${pageSize})`)
                updatePageCount(pageCount)
            } else {
                console.log("null rowCount, skipping updatePageCount")
            }
        },
        [ pageSize, rowCount, ]
    )

    function getColumnSortChar(column: string) {
        const desc = sorts.find((sort) => sort.column == column)?.desc
        return desc === true ? ' ⬇' : desc === false ? ' ⬆' : ''
    }

    // Render the UI for your table
    return (
        <>
            <div className="table" {...getTableProps()}>
                <div>
                {headerGroups.map(headerGroup => (
                    <div {...headerGroup.getHeaderGroupProps()} className="tr">
                        {headerGroup.headers.map(column => (
                            // Add the sorting props to control sorting. For this example
                            // we can add them into the header props
                            <div
                                className="th"
                                {...column.getHeaderProps()}
                                onClick={() => handleHeaderClick(column.id)}
                            >
                                {column.render('Header')}
                                <span>{getColumnSortChar(column.id)}</span>
                                <div
                                    {...column.getResizerProps()}
                                    className={`resizer ${
                                        column.isResizing ? 'isResizing' : ''
                                    }`}
                                    onClick={(e) => e.stopPropagation()}
                                />
                            </div>
                        ))}
                    </div>
                ))}
                </div>
                <div {...getTableBodyProps()}>
                {page.map((row, i) => {
                    prepareRow(row)
                    return (
                        <div className="tr" {...row.getRowProps()}>
                            {row.cells.map(cell => {
                                return (
                                    <div className="td"
                                        {...cell.getCellProps()}
                                        onClick={
                                            (e) => {
                                                console.log("cell:", cell, "target:", e.target)
                                                handleCellClick(cell.column.id, cell.value)
                                            }
                                        }
                                    >
                                        {cell.render('Cell')}
                                    </div>
                                )
                            })}
                        </div>
                    )
                })}
                </div>
            </div>
            <div className="pagination">
                <button onClick={() => gotoPage(0)} disabled={!canPreviousPage}>
                    {'<<'}
                </button>{' '}
                <button onClick={() => previousPage()} disabled={!canPreviousPage}>
                    {'<'}
                </button>{' '}
                <button onClick={() => nextPage()} disabled={!canNextPage}>
                    {'>'}
                </button>{' '}
                <button onClick={() => gotoPage(pageCount === null ? 0 : (pageCount - 1))} disabled={!canNextPage}>
                    {'>>'}
                </button>{' '}
                <span>
                    Page{' '}
                    <strong>
                        {pageIndex + 1} of {pageOptions.length}
                    </strong>{' '}
                </span>
                <span>
                    | Go to page:{' '}
                    <input
                        type="number"
                        defaultValue={pageIndex + 1}
                        onChange={e => {
                            const page = e.target.value ? Number(e.target.value) - 1 : 0
                            gotoPage(page)
                        }}
                        style={{ width: '100px' }}
                    />
                </span>{' '}
                <select
                    value={pageSize}
                    onChange={e => {
                        setPageSize(Number(e.target.value))
                    }}
                >
                    {[10, 20, 30, 40, 50].map(pageSize => (
                        <option key={pageSize} value={pageSize}>
                            Show {pageSize}
                        </option>
                    ))}
                </select>
            </div>
            <pre>
                <code>
                    {JSON.stringify(
                        {
                            pageIndex,
                            pageSize,
                            pageCount,
                            canNextPage,
                            canPreviousPage,
                        },
                        null,
                        2
                    )}
                </code>
            </pre>
        </>
    )
}
