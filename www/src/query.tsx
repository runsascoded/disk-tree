import {Filter, Sort} from "./table";

export function build(
    {
        table,
        limit,
        offset,
        count,
        sorts,
        filters,
    }: {
        table: string,
        limit?: number,
        offset?: number,
        count?: string,
        sorts?: Sort[],
        filters?: Filter[],
    }
) {
    let query =
        count ?
            `SELECT count(*) AS ${count} FROM ${table}` :
            `SELECT * FROM ${table}`;
    if (filters?.length) {
        query += ' WHERE'
        let first = true
        for (const { column, value } of filters) {
            if (!first) query += ' AND'
            first = false
            // TODO: escape values; normally this would be a serious vulnerability, but this is a static site reading a
            //  read-only SQLite database, so believed to be benign
            query += ` ${column} = "${value}"`
        }
    }
    if (sorts?.length) {
        query += ` ORDER BY`
        let first = true
        for (const sort of sorts) {
            if (!first) query += ','
            first = false
            query += ` ${sort.column}`
            query += sort.desc ? " DESC" : " ASC"
        }
    }
    if (limit !== undefined) query += ` LIMIT ${limit}`
    if (offset !== undefined) query += ` OFFSET ${offset}`
    return query
}