import {HashRouter, Route, Routes} from "react-router-dom";
import React, {useState,} from 'react';
import ReactDOM from 'react-dom'

import $ from 'jquery';
import {DiskTree} from "./disk-tree";
import {List} from "./list";
import {Worker} from "./worker";

function Router() {
    const [ url, setUrl ] = useState("/assets/www.db");
    const [ worker, setWorker ] = useState<Worker>(new Worker({ url, }))

    return (
            <HashRouter>
                <Routes>
                    <Route path="" element={<List url={url} worker={worker} />} />
                    <Route path="disk-tree" element={<DiskTree url={url} worker={worker} />} />
                </Routes>
            </HashRouter>
    )
}

$(document).ready(function () {
    ReactDOM.render(<Router/>, document.getElementById('root'));
});
