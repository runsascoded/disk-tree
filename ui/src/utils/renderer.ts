/**
 * Treemap renderer preference: `'dom'` (the mature absolutely-positioned-`<div>`
 * renderer) vs `'canvas'` (`@disk-tree/react`'s single-`<canvas>` renderer — one
 * paint loop, no DOM node per cell, progressive paint). Same module-store +
 * `useSyncExternalStore` shape as `tiling.ts`, persisted in localStorage.
 * Defaults to `'dom'`; the treemap's legend exposes a toggle so canvas can be
 * profiled on real scans before it's ever a default.
 */
import { useSyncExternalStore } from 'react'

type Renderer = 'dom' | 'canvas'

const STORAGE_KEY = 'disk-tree:renderer'

function load(): Renderer {
  try {
    const v = localStorage.getItem(STORAGE_KEY)
    if (v === 'dom' || v === 'canvas') return v
  } catch {
    // no storage — default
  }
  return 'dom'
}

let current: Renderer = load()
const listeners = new Set<() => void>()

export function getRenderer(): Renderer {
  return current
}

export function setRenderer(r: Renderer): void {
  if (r === current) return
  current = r
  try {
    localStorage.setItem(STORAGE_KEY, r)
  } catch {
    // in-memory only
  }
  listeners.forEach(l => l())
}

function subscribe(l: () => void): () => void {
  listeners.add(l)
  return () => { listeners.delete(l) }
}

export function useRenderer(): [Renderer, (r: Renderer) => void] {
  const r = useSyncExternalStore(subscribe, getRenderer, getRenderer)
  return [r, setRenderer]
}
