import {Setter} from "./utils";
import {useEffect, useState} from "react";
import {NavigateFunction, useLocation, useNavigate} from "react-router-dom";
import _ from "lodash";

export type Parse<T> = (queryParam: string) => T
export type Render<T> = (value: T) => string
export type Eq<T> = (l: T, r: T) => boolean

export abstract class QueryState<T> {
    render(value: T): string {
        return (value as any).toString()
    }
    eq(l: T, r: T): boolean {
        return l == r
    }
    abstract parse(queryParams: string): T
    abstract defaultValue: T
}

export class StringQueryState extends QueryState<string> {
    readonly defaultValue: string = ""
    parse(queryParam: string): string { return queryParam }
}

export const stringQueryState = new StringQueryState()

export function queryParamToState<T>(
    { queryKey, queryValue, state, setState, defaultValue, parse }: {
        queryKey: string,
        queryValue: string | null,
        state: T | null,
        setState: Setter <T | null>,
        defaultValue: T,
        parse?: Parse<T>,
    }
) {
    useEffect(
        () => {
            if (state === null) {
                if (queryValue) {
                    const parsedState = parse ? parse(queryValue) : (queryValue as any as T)
                    console.log(`queryKey ${queryKey} = ${queryValue}: parsed`, parsedState)
                    setState(parsedState)
                } else {
                    console.log(`queryKey ${queryKey} = ${queryValue}: setting default value`, defaultValue)
                    setState(defaultValue)
                }
            }
        },
        [ queryValue ]
    )
}

export const defaultReplaceChars = { '%2F': '/', '%21': '!', }

export function stateToQueryParam<T>(
    { queryKey, state, searchParams, navigate, defaultValue, render, eq, replaceChars, }: {
        queryKey: string,
        queryValue: string | null,
        state: T | null,
        searchParams: URLSearchParams,
        navigate: NavigateFunction,
        defaultValue: T,
        render?: Render<T>,
        eq?: Eq<T>,
        replaceChars?: { [k: string]: string, },
    }
) {
    useEffect(
        () => {
            if (state === null) return
            if (
                eq ?
                    eq(state, defaultValue) :
                    typeof state === 'object' ?
                        _.isEqual(state, defaultValue) :
                        (state == defaultValue)
            ) {
                searchParams.delete(queryKey)
            } else {
                const queryValue = render ? render(state) : (state as any).toString()
                searchParams.set(queryKey, queryValue)
            }
            const queryString = getQueryString({ searchParams, replaceChars })
            console.log(`queryKey ${queryKey} new string`, queryString)
            navigate(
                {
                    pathname: "",
                    search: queryString,
                },
                { replace: true, },
            )
        },
        [ state ]
    )
}

export function getQueryString(
    { searchParams, replaceChars }: {
        searchParams?: URLSearchParams,
        replaceChars?: { [k: string]: string },
    }
) {
    if (!searchParams) {
        const { search: query } = useLocation();
        searchParams = new URLSearchParams(query)
    }
    let queryString = searchParams.toString()
    replaceChars = replaceChars || defaultReplaceChars
    Object.entries(replaceChars).forEach(([ k, v ]) => {
        queryString = queryString.replaceAll(k, v)
    })
    return queryString
}

export function fromQueryString({ query }: { query?: string, }): { [k: string]: string } {
    if (query === undefined) {
        query = useLocation().search
    }
    const searchParams = new URLSearchParams(query)
    return Object.fromEntries(searchParams.entries())
}

export function toQueryString(o: { [k: string]: string }): string {
    let searchParams = new URLSearchParams()
    Object.entries(o).forEach(([ k, v ]) => {
        searchParams.set(k, v)
    })
    const queryString = getQueryString({searchParams})
    return queryString
}

export function assignQueryString(
    { query, o }: {
        query?: string,
        o: { [k: string]: string }
    }
): string {
    return toQueryString(
        Object.assign(
            {},
            fromQueryString({ query }),
            o,
        )
    )
}

export function useQueryState<T>(
    queryKey: string,
    queryState: QueryState<T>,
): [ T | null, Setter<T | null>, string | null, ] {
    const { defaultValue, parse, render, eq, } = queryState
    const { search: query } = useLocation();
    const searchParams = new URLSearchParams(query)
    const queryValue = searchParams.get(queryKey)
    let navigate = useNavigate()
    const [ state, setState ] = useState<T | null>(null)

    queryParamToState<T>({
        queryKey,
        queryValue,
        state,
        setState,
        defaultValue,
        parse,
    })

    stateToQueryParam<T>({
        queryKey,
        queryValue,
        state,
        defaultValue,
        render,
        searchParams,
        navigate,
        eq,
    })
    return [ state, setState, queryValue ]
}
