import { useCallback, useEffect, useState } from 'react'

export type ViewType = 'tree' | 'compare'

export type RecentPath = {
  uri: string
  visitedAt: number  // timestamp ms
  viewType: ViewType
}

const STORAGE_KEY = 'disk-tree-recent-paths'
const MAX_RECENT = 50

function loadRecent(): RecentPath[] {
  try {
    const stored = localStorage.getItem(STORAGE_KEY)
    const parsed = stored ? JSON.parse(stored) : []
    // Migrate old entries without viewType
    return parsed.map((p: RecentPath) => ({
      ...p,
      viewType: p.viewType || 'tree',
    }))
  } catch {
    return []
  }
}

function saveRecent(paths: RecentPath[]): void {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(paths))
}

export function useRecentPaths() {
  const [recent, setRecent] = useState<RecentPath[]>(() => loadRecent())

  // Sync from storage on mount and when other tabs change storage
  useEffect(() => {
    const handleStorage = (e: StorageEvent) => {
      if (e.key === STORAGE_KEY) {
        setRecent(loadRecent())
      }
    }
    window.addEventListener('storage', handleStorage)
    return () => window.removeEventListener('storage', handleStorage)
  }, [])

  const recordVisit = useCallback((uri: string, viewType: ViewType = 'tree') => {
    setRecent(prev => {
      // Remove existing entry for this URI + viewType combo
      const filtered = prev.filter(p => !(p.uri === uri && p.viewType === viewType))
      // Add to front with current timestamp
      const updated = [{ uri, visitedAt: Date.now(), viewType }, ...filtered].slice(0, MAX_RECENT)
      saveRecent(updated)
      return updated
    })
  }, [])

  const clearRecent = useCallback(() => {
    setRecent([])
    localStorage.removeItem(STORAGE_KEY)
  }, [])

  return { recent, recordVisit, clearRecent }
}
