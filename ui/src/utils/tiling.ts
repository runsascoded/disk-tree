/**
 * Treemap tiling preference: `'gaps'` (2px gutters, rounded — the classic
 * look) vs `'shared'` (cells share edges, one stroke per boundary — exact
 * areas). Same module-store + `useSyncExternalStore` shape as `units.ts`,
 * persisted in localStorage; every treemap in the app reads it.
 */
import { useSyncExternalStore } from 'react'
import type { Tiling } from '@disk-tree/react'

const STORAGE_KEY = 'disk-tree:tiling'

function load(): Tiling {
  try {
    const v = localStorage.getItem(STORAGE_KEY)
    if (v === 'gaps' || v === 'shared') return v
  } catch {
    // no storage — default
  }
  return 'shared'
}

let current: Tiling = load()
const listeners = new Set<() => void>()

export function getTiling(): Tiling {
  return current
}

export function setTiling(t: Tiling): void {
  if (t === current) return
  current = t
  try {
    localStorage.setItem(STORAGE_KEY, t)
  } catch {
    // in-memory only
  }
  listeners.forEach(l => l())
}

function subscribe(l: () => void): () => void {
  listeners.add(l)
  return () => { listeners.delete(l) }
}

export function useTiling(): [Tiling, (t: Tiling) => void] {
  const t = useSyncExternalStore(subscribe, getTiling, getTiling)
  return [t, setTiling]
}
