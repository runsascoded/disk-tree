import { createDbWorker } from "sql.js-httpvfs";
import React, {useEffect, useState} from 'react';
import ReactDOM from 'react-dom'
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


import { Column, useTable, usePagination, } from 'react-table'

function useAsyncHook(url: string): [ Row[], string ] {
    const [ data, setData ] = useState<Row[]>([])
    const [loading, setLoading] = React.useState("false");

    React.useEffect(
        () => {
            async function fetchDB() {
                try {
                    setLoading("true");
                    console.log("Fetching DB…", url);
                    const worker = await createDbWorker(
                        [
                            {
                                from: "inline",
                                config: {
                                    serverMode: "full",
                                    url: url,
                                    requestChunkSize: 4096,
                                },
                            },
                        ],
                        workerUrl.toString(),
                        wasmUrl.toString()
                    );
                    const rows = (await worker.db.query(`select * from file limit 100`)) as Row[];
                    console.log("Fetched db:", url);
                    console.log("data:", rows);
                    setData(rows)
                } catch (error) {
                    setLoading("null");
                }
            }

            if (url !== "") {
                fetchDB();
            }
        },
        [url]
    );

    return [data, loading];
}

function Table({ columns, data }: { columns: Column<Row>[], data: Row[] }) {
    // Use the state and functions returned from useTable to build your UI
    const {
        getTableProps,
        getTableBodyProps,
        headerGroups,
        prepareRow,
        page, // Instead of using 'rows', we'll use page,
        // which has only the rows for the active page

        // The rest of these things are super handy, too ;)
        canPreviousPage,
        canNextPage,
        pageOptions,
        pageCount,
        gotoPage,
        nextPage,
        previousPage,
        setPageSize,
        state: { pageIndex, pageSize },
    } = useTable<Row>(
        {
            columns,
            data,
            initialState: { pageIndex: 0, pageSize: 20, },
        },
        usePagination
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
                            <th {...column.getHeaderProps()}>{column.render('Header')}</th>
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
            {/*
        Pagination can be built however you'd like.
        This is just a very basic UI implementation:
      */}
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
                <button onClick={() => gotoPage(pageCount - 1)} disabled={!canNextPage}>
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

function App1() {
    const url = "/assets/disk-tree.db";
    const [data, loading] = useAsyncHook(url);

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

    // const {
    //     getTableProps,
    //     getTableBodyProps,
    //     headerGroups,
    //     rows,
    //     prepareRow,
    // } = useTable({ columns, data })

    return (
        <Table columns={columns} data={data} />
        // <table {...getTableProps()} style={{ border: 'solid 1px blue' }}>
        //     <thead>
        //     {headerGroups.map(headerGroup => (
        //         <tr {...headerGroup.getHeaderGroupProps()}>
        //             {headerGroup.headers.map(column => (
        //                 <th
        //                     {...column.getHeaderProps()}
        //                     style={{
        //                         borderBottom: 'solid 3px red',
        //                         background: 'aliceblue',
        //                         color: 'black',
        //                         fontWeight: 'bold',
        //                     }}
        //                 >
        //                     {column.render('Header')}
        //                 </th>
        //             ))}
        //         </tr>
        //     ))}
        //     </thead>
        //     <tbody {...getTableBodyProps()}>
        //     {rows.map(row => {
        //         prepareRow(row)
        //         return (
        //             <tr {...row.getRowProps()}>
        //                 {row.cells.map(cell => {
        //                     return (
        //                         <td
        //                             {...cell.getCellProps()}
        //                             style={{
        //                                 padding: '10px',
        //                                 border: 'solid 1px gray',
        //                                 background: 'papayawhip',
        //                             }}
        //                         >
        //                             {cell.render('Cell')}
        //                         </td>
        //                     )
        //                 })}
        //             </tr>
        //         )
        //     })}
        //     </tbody>
        // </table>
    )
}

$(document).ready(function () {
    ReactDOM.render(<App1 />, document.getElementById('root'));
});
