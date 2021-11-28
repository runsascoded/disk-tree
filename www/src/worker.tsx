import {WorkerHttpvfs} from "sql.js-httpvfs/dist/db";
import {createDbWorker} from "sql.js-httpvfs";
import {useEffect, useState} from "react";
import {Row} from "./data";
import {build, Filter, Sort} from "./query";
import {Setter} from "./utils";

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

type Opts = {
    url: string
    chunkSize?: number
    ready?: (worker: WorkerHttpvfs) => void
}
export class Worker {
    url: string
    chunkSize: number
    worker: Promise<WorkerHttpvfs>
    ready?: (worker: WorkerHttpvfs) => void
    requests: { [query: string]: { promise?: Promise<any> } }

    constructor({ url, chunkSize = DefaultChunkSize, ready }: Opts) {
        this.url = url
        this.chunkSize = chunkSize
        this.ready = ready
        this.worker = this.initWorker()
        this.requests = {}
    }

    async initWorker(): Promise<WorkerHttpvfs> {
        const { url, chunkSize } = this
        try {
            console.log("Fetching DB…", this.url);
            const worker: Promise<WorkerHttpvfs> = createDbWorker(
                [
                    {
                        from: "inline",
                        config: {
                            serverMode: "full",
                            url,
                            requestChunkSize: chunkSize,
                        },
                    },
                ],
                workerUrl.toString(),
                wasmUrl.toString()
            );
            console.log("setting worker")
            worker.then(this.ready)
            return worker
        } catch (error) {
            throw error
        }
    }

    async count({ table, limit, offset, filters }: {
        table: string,
        limit?: number,
        offset?: number,
        filters?: Filter[],
    }): Promise<number> {
        const query = build(
            {
                table,
                limit,
                offset,
                count: 'rowCount',
                filters,
            }
        )
        if (query in this.requests) {
            const promise = this.requests[query].promise as Promise<number>
            if (!promise) {
                throw Error(`Promise race: ${query}`)
            }
            return promise
        }
        this.requests[query] = {}
        console.log("query:", query)
        const count = (
            this.worker
                .then((worker) => worker.db.query(query))
                .then((rows) => {
                    delete this.requests[query]
                    return rows as {  rowCount: number }[]
                })
                .then(([ { rowCount } ]) => rowCount)
        )
        this.requests[query].promise = count
        return count
    }

    async fetch<Row>({ table, limit, offset, sorts, filters, }: {
        table: string,
        limit?: number,
        offset?: number,
        sorts?: Sort[],
        filters?: Filter[],
    }): Promise<Row[]> {
        const query = build(
            {
                table,
                limit,
                offset,
                sorts,
                filters,
            }
        )
        if (query in this.requests) {
            const promise = this.requests[query].promise as Promise<Row[]>
            if (!promise) {
                throw Error(`Promise race: ${query}`)
            }
            return promise
        }
        console.log("query:", query)
        this.requests[query] = {}
        const rows = (
            this.worker
                .then((worker) => worker.db.query(query))
                .then((rows) => {
                    delete this.requests[query]
                    return rows as Row[]
                })
        )
        this.requests[query].promise = rows
        return rows
    }
}
