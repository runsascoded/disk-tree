import {createDbWorker} from "sql.js-httpvfs";
import {HashRouter, Route, Routes} from "react-router-dom";
import React, {useEffect, useState,} from 'react';
import ReactDOM from 'react-dom'

import $ from 'jquery';
import {WorkerHttpvfs,} from "sql.js-httpvfs/dist/db";
import {DiskTree} from "./disk-tree";
import {App} from "./app";

const workerUrl = new URL(
    "sql.js-httpvfs/dist/sqlite.worker.js",
    import.meta.url
);
const wasmUrl = new URL("sql.js-httpvfs/dist/sql-wasm.wasm", import.meta.url);

function Router() {
    const url: string = "/assets/disk-tree1k.db";
    const [ worker, setWorker ] = useState<WorkerHttpvfs | null>(null)
    const [ loadingWorker, setLoadingWorker ] = useState(false);

    // URL -> Worker
    useEffect(
        () => {
            console.log("effect; loadingWorker:", loadingWorker)
            async function initWorker() {
                console.log("initWorker; loading:", loadingWorker)
                if (loadingWorker) {
                    console.log("skipping init, loading already true")
                    return
                }
                try {
                    setLoadingWorker(true);
                    console.log("Fetching DB…", url);
                    const worker: WorkerHttpvfs = await createDbWorker(
                        [
                            {
                                from: "inline",
                                config: {
                                    serverMode: "full",
                                    url: url,
                                    //requestChunkSize: 4096,
                                    requestChunkSize: 8 * 1048576,
                                },
                            },
                        ],
                        workerUrl.toString(),
                        wasmUrl.toString()
                    );
                    console.log("setting worker")
                    setWorker(worker)
                } catch (error) {
                    setLoadingWorker(false);
                }
            }

            if (url !== "") {
                initWorker();
            }
        },
        [url]
    );

    return (
        <HashRouter>
            <Routes>
                <Route path="" element={<App url={url} worker={worker} />} />
                <Route path="disk-tree" element={<DiskTree url={url} worker={worker} />} />
            </Routes>
        </HashRouter>
    )
}

$(document).ready(function () {
    ReactDOM.render(<Router/>, document.getElementById('root'));
});
