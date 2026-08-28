/**
 * Size-unit preference: SI (`G` = 10⁹, default — matches Finder/`df -H`) vs
 * IEC (`Gi` = 2³⁰, what the CLI's `naturalsize(binary=True)` prints). Module store +
 * `useSyncExternalStore` so `formatSize` (a plain function, called from
 * everywhere) reads the live value, and subscribed components re-render on
 * toggle. Persisted in localStorage.
 */
import { useSyncExternalStore } from 'react'

export type Units = 'si' | 'iec'

const STORAGE_KEY = 'disk-tree:units'

function load(): Units {
  try {
    const v = localStorage.getItem(STORAGE_KEY)
    if (v === 'si' || v === 'iec') return v
  } catch {
    // no storage (private mode, SSR) — fall through to default
  }
  return 'si'
}

let current: Units = load()
const listeners = new Set<() => void>()

export function getUnits(): Units {
  return current
}

export function setUnits(u: Units): void {
  if (u === current) return
  current = u
  try {
    localStorage.setItem(STORAGE_KEY, u)
  } catch {
    // storage unavailable — in-memory only
  }
  listeners.forEach(l => l())
}

function subscribe(l: () => void): () => void {
  listeners.add(l)
  return () => { listeners.delete(l) }
}

/** Subscribe a component to the units preference (re-renders on toggle). */
export function useUnits(): [Units, (u: Units) => void] {
  const u = useSyncExternalStore(subscribe, getUnits, getUnits)
  return [u, setUnits]
}
