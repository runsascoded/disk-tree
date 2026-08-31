import { useMemo } from 'react'
import { FileTree } from '@rdub/file-tree/react'
import { TreeMapView } from '@rdub/file-tree/renderers/treemap'
import { useUrlPersistedState } from '@rdub/file-tree/url-state'
import { diskTreeStore, diskTreeTreeSource } from '../filetree/adapters'

/** Phase-1 proof of Half A (spec `half-a-adopt-filetree.md`): the raw browse
 *  surface rendered by `@rdub/file-tree`'s `<FileTree>` instead of the
 *  hand-rolled table, backed by disk-tree's own API — directory rows get their
 *  recursive size from a scan (via `diskTreeTreeSource`) and the list↔treemap
 *  toggle renders `@disk-tree/react`'s `<Treemap>`. Read-only; nothing is
 *  retired yet. Rooted at the filesystem root, so `/browse/Users/ryan` shows
 *  that scanned subtree.
 */
export function BrowsePage() {
  const rootUri = '/'
  const store = useMemo(() => diskTreeStore(rootUri), [])
  const treeSource = useMemo(() => diskTreeTreeSource(rootUri), [])
  return (
    <FileTree
      store={store}
      routeBase="/browse"
      title="Browse — FileTree ⇄ disk-tree"
      treeSource={treeSource}
      treemapRenderer={TreeMapView}
      usePersistedState={useUrlPersistedState}
    />
  )
}
