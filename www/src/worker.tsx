import {WorkerHttpvfs} from "sql.js-httpvfs/dist/db";
import {createDbWorker} from "sql.js-httpvfs";
import {useEffect, useState} from "react";

export const workerUrl = new URL(
    "sql.js-httpvfs/dist/sqlite.worker.js",
    import.meta.url
);
export const wasmUrl = new URL("sql.js-httpvfs/dist/sql-wasm.wasm", import.meta.url);
export const DefaultChunkSize = 8 * 1024 * 1024
export function SQLWorker(
    { url, chunkSize, handleWorker, children }: {
        url: string,
        chunkSize?: number,
        handleWorker?: (worker: WorkerHttpvfs) => void,
        children: any,
    }) {
    const [ worker, setWorker ] = useState<WorkerHttpvfs | null>(null)
    const [ loadingWorker, setLoadingWorker ] = useState(false);

    let requestChunkSize = chunkSize || DefaultChunkSize

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
                                    url,
                                    requestChunkSize,
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

    useEffect(
        () => {
            if (worker && handleWorker) {
                handleWorker(worker)
            }
        },
        [ worker, handleWorker ]
    )
    return children({ worker })
}
