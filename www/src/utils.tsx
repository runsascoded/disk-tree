import React from "react";

const { round } = Math

export type Setter<T> = React.Dispatch<React.SetStateAction<T>>

export const renderSize = function(size: number): string {
    const orders = [ 'K', 'M', 'G', 'T', 'P', 'E', ]
    const iec = true
    const [ suffix, base ]: [ string, number, ] = iec ? ['iB', 1024, ] : ['B', 1000]
    const [ n, o, ] = orders.reduce<[ number, string, ]>(
        ([size, curOrder], nxtOrder) => {
            if (size < base) {
                return [ size, curOrder ]
            } else {
                return [ size / base, nxtOrder]
            }
        },
        [ size, '', ],
    )
    if (!o) {
        return `${n}B`
    }
    const rendered = n >= 100 ? round(n).toString() : n.toFixed(1)
    return `${rendered}${o}${suffix}`
}

export const basename = function(path: string): string {
    const idx = path.lastIndexOf('/')
    return idx == -1 ? path : path.substring(idx + 1)
}
