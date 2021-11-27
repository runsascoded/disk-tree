import {WorkerHttpvfs} from "sql.js-httpvfs/dist/db";
import {useLocation} from "react-router-dom";
import {build} from "./query";
import React, {useState} from "react";
import {Row} from "./data";

export function DiskTree({ url, worker }: { url: string, worker: WorkerHttpvfs | null }) {
    console.log("location:", useLocation())
    const query = build({
        table: 'file',
        limit: 2000,
        sorts: [{ column: 'size', desc: true, }]
    })
    const [ data, setData ] = useState<Row[]>([])
    //await worker.db.query(query)
    return <div>yay {url}</div>
}
