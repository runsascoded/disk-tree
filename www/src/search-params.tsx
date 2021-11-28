import {Setter} from "./utils";
import {useEffect} from "react";
import {NavigateFunction} from "react-router-dom";
import _ from "lodash";

export function queryParamToState<T>(
    { queryKey, queryValue, state, setState, defaultValue, parse }: {
        queryKey: string,
        queryValue: string | null,
        state: T | null,
        setState: Setter <T | null>,
        defaultValue: T,
        parse?: (queryParam: string) => T,
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

export function stateToQueryParam<T>(
    { queryKey, state, searchParams, navigate, defaultValue, render, eq, replaceChars, }: {
        queryKey: string,
        queryValue: string | null,
        state: T | null,
        searchParams: URLSearchParams,
        navigate: NavigateFunction,
        defaultValue: T,
        render?: (value: T | null) => string,
        eq?: (l: T, r: T) => boolean,
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
            let queryString = searchParams.toString()
            replaceChars = replaceChars || { '%2F': '/', '%21': '!', }
            Object.entries(replaceChars).forEach(([ k, v ]) => {
                queryString = queryString.replaceAll(k, v)
            })
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
