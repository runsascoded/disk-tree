import {HashRouter, Route, Routes} from "react-router-dom";
import React, {useState,} from 'react';
import ReactDOM from 'react-dom'

import $ from 'jquery';
import {WorkerHttpvfs} from "sql.js-httpvfs/dist/db";
import {DiskTree} from "./disk-tree";
import {List} from "./list";
import {SQLWorker} from "./worker";

function Router() {
    const [ url, setUrl ] = useState("/assets/disk-tree1k.db");

    return (
        <SQLWorker url={url}>
            {
                (renderProps: { worker: WorkerHttpvfs, }) => {
                    const worker: WorkerHttpvfs = renderProps.worker
                    console.log("renderProps:", renderProps)
                    return (
                        <HashRouter>
                            <Routes>
                                <Route path="" element={<List url={url} worker={worker} />} />
                                <Route path="disk-tree" element={<DiskTree url={url} worker={worker} />} />
                            </Routes>
                        </HashRouter>
                    )
                }
            }
        </SQLWorker>
    )
}

$(document).ready(function () {
    ReactDOM.render(<Router/>, document.getElementById('root'));
});
