import {Column, useBlockLayout, usePagination, useResizeColumns, useTable} from "react-table";
import React, {useEffect} from "react";
import { Setter } from "./utils";
import {Worker} from "./worker";

export type Sort = { column: string, desc: boolean, }
export type Filter = { column: string, value: string, }

const { ceil, } = Math

export function Table<Row extends object>(
    {
        columns,
        data,
        setData,
        sorts,
        filters,
        handleHeaderClick,
        handleCellClick,
        getColumnProps,
        rowCount,
        pageCount,
        worker,
        updatePageCount,
        initialPageSize,
    }: {
        columns: Column<Row>[],
        data: Row[],
        setData: (rows: Row[]) => void,
        sorts: Sort[],
        filters: Filter[],
        handleHeaderClick: (column: string) => void,
        handleCellClick: (column: string, value: string) => void,
        getColumnProps: (column: Column<Row>) => object,
        pageCount: number,
        updatePageCount: Setter<number>,
        rowCount: number | null,
        worker: Worker,
        initialPageSize: number
    }
) {
    const defaultColumn = React.useMemo(
        () => ({
            minWidth: 50,
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
            worker.fetch<Row>({
                table: 'file',
                limit: pageSize,
                offset: pageIndex * pageSize,
                sorts,
                filters,
            }).then(setData)
        },
        [ worker, pageIndex, pageSize, sorts, filters, ],
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
                const pageCount = ceil(rowCount / pageSize)
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

    return (
        <>
            <div className="table" {...getTableProps()}>
                <div>
                {headerGroups.map(headerGroup => (
                    <div {...headerGroup.getHeaderGroupProps()} className="tr">
                        {headerGroup.headers.map(column => (
                            <div
                                className="th"
                                {...column.getHeaderProps(getColumnProps(column))}
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
                {page.map(row => {
                    prepareRow(row)
                    return (
                        <div className="tr" {...row.getRowProps()}>
                            {row.cells.map(cell =>
                                    <div
                                        className="td"
                                        {...cell.getCellProps(getColumnProps(cell.column))}
                                        onClick={() => handleCellClick(cell.column.id, cell.value)}
                                    >
                                        {cell.render('Cell')}
                                    </div>
                            )}
                        </div>
                    )
                })}
                </div>
            </div>
            <div className="pagination">
                <button onClick={() => gotoPage(0)} disabled={!canPreviousPage}>{'<<'}</button>{' '}
                <button onClick={() => previousPage()} disabled={!canPreviousPage}>{'<'}</button>{' '}
                <button onClick={() => nextPage()} disabled={!canNextPage}>{'>'}</button>{' '}
                <button onClick={() => gotoPage(pageCount === null ? 0 : (pageCount - 1))} disabled={!canNextPage}>{'>>'}</button>{' '}
                <span className="page-number">
                    Page{' '}
                    <span>{pageIndex + 1} of {pageOptions.length}</span>{' '}
                </span>
                <span className="goto-page">| Go to page:{' '}</span>
                <input
                    type="number"
                    defaultValue={pageIndex + 1}
                    onChange={e => gotoPage(e.target.value ? Number(e.target.value) - 1 : 0)}
                    style={{ width: '100px' }}
                />
                {' '}
                <select
                    value={pageSize}
                    onChange={e => setPageSize(Number(e.target.value))}
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
