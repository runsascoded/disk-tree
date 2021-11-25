import {Column, usePagination, useTable} from "react-table";
import {WorkerHttpvfs} from "sql.js-httpvfs/dist/db";
import React, {useEffect} from "react";
import { Setter } from "./utils";

export type Sort = { column: string, desc: boolean, }

export function Table<Row extends object>(
    {
        columns,
        data,
        fetchData,
        sort,
        handleHeaderClick,
        rowCount,
        pageCount,
        worker,
        updatePageCount,
        initialPageSize,
    }: {
        columns: Column<Row>[],
        data: Row[],
        fetchData: (
            { worker, pageSize, pageIndex, sort }: {
                worker: WorkerHttpvfs,
                pageSize: number,
                pageIndex: number,
                sort?: Sort,
            }
        ) => void,
        sort?: Sort,
        handleHeaderClick: (column: string) => void,
        pageCount: number,
        updatePageCount: Setter<number>,
        rowCount: number | null,
        worker: WorkerHttpvfs | null,
        initialPageSize: number
    }
) {
    // Use the state and functions returned from useTable to build your UI
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
        },
        usePagination,
    )
    // Listen for changes in pagination and use the state to fetch our new data
    useEffect(
        () => {
            if (worker !== null) {
                console.log("table fetching")
                fetchData({ worker, pageIndex, pageSize, })
            } else {
                console.log("null worker")
            }
        },
        [ fetchData, worker, pageIndex, pageSize, ],
    )

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

    // Render the UI for your table
    return (
        <>
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
            <table {...getTableProps()}>
                <thead>
                {headerGroups.map(headerGroup => (
                    <tr {...headerGroup.getHeaderGroupProps()}>
                        {headerGroup.headers.map(column => (
                            // Add the sorting props to control sorting. For this example
                            // we can add them into the header props
                            <th {...column.getHeaderProps()} onClick={() => handleHeaderClick(column.id)}>
                                {column.render('Header')}
                                <span>
                                    {sort && sort.column == column.id
                                        ? sort && sort.desc
                                            ? ' 🔽'
                                            : ' 🔼'
                                        : ''}
                                </span>
                            </th>
                        ))}
                    </tr>
                ))}
                </thead>
                <tbody {...getTableBodyProps()}>
                {page.map((row, i) => {
                    prepareRow(row)
                    return (
                        <tr {...row.getRowProps()}>
                            {row.cells.map(cell => {
                                return <td {...cell.getCellProps()}>{cell.render('Cell')}</td>
                            })}
                        </tr>
                    )
                })}
                </tbody>
            </table>
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
        </>
    )
}
