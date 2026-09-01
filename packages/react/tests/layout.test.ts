import { describe, expect, it } from 'vitest'
import { flattenPlaced, hitTest, layoutCells, type LayoutConfig, type PlacedCell } from '../src/layout'
import { squarify } from '../src/squarify'

interface N {
  n: string
  size: number
  children?: N[]
}

const getSize = (n: N) => n.size
const getLabel = (n: N) => n.n
const childrenOf = (n: N) => n.children

/** A no-fold, gaps-tiling config — the layout primitives with everything off. */
const cfg: LayoutConfig<N> = {
  getSize,
  getLabel,
  childrenOf,
  showLabels: true,
  collapseChains: false,
  borderWidth: () => 2,
  fold: raw => raw,
  layTiles: (items, x, y, w, h) => squarify(items, x, y, w, h, it => (it as N).size),
  tilingFor: () => 'gaps',
}

const tree: N = {
  n: 'root',
  size: 300,
  children: [
    {
      n: 'foo',
      size: 200,
      children: [
        { n: 'a.txt', size: 120 },
        { n: 'b.txt', size: 80 },
      ],
    },
    { n: 'bar', size: 100 },
  ],
}

const W = 400
const H = 300

/** Lay the whole tree: squarify the top level, then recurse via layoutCells. */
function place(): PlacedCell<N>[] {
  const top = squarify(tree.children!, 0, 0, W, H, getSize)
  return layoutCells(top, [tree], 'gaps', cfg)
}

const label = (c: PlacedCell<N>) => (c.node as N).n

describe('layoutCells', () => {
  it('places the top level with the viewed node as basePath', () => {
    const cells = place()
    expect(cells.map(c => [label(c), c.depth, c.path.map(getLabel)])).toEqual([
      ['foo', 0, ['root', 'foo']],
      ['bar', 0, ['root', 'bar']],
    ])
  })

  it('recurses children under the branch, one depth down', () => {
    const foo = place()[0]
    expect(foo.children.map(c => [label(c), c.depth, c.path.map(getLabel)])).toEqual([
      ['a.txt', 1, ['root', 'foo', 'a.txt']],
      ['b.txt', 1, ['root', 'foo', 'b.txt']],
    ])
  })

  it('does not recurse a leaf', () => {
    const bar = place()[1]
    expect(bar.children).toEqual([])
    expect(bar.hasKids).toBe(false)
  })

  it('nests every child rect inside its parent box', () => {
    const foo = place()[0]
    for (const kid of foo.children) {
      expect(kid.x).toBeGreaterThanOrEqual(foo.x)
      expect(kid.y).toBeGreaterThanOrEqual(foo.y)
      expect(kid.x + kid.w).toBeLessThanOrEqual(foo.x + foo.w + 0.01)
      expect(kid.y + kid.h).toBeLessThanOrEqual(foo.y + foo.h + 0.01)
    }
  })

  it('offsets children below the 20px title bar', () => {
    const foo = place()[0]
    // Children start at the parent top + the title-bar inset, never overlapping it.
    expect(Math.min(...foo.children.map(k => k.y))).toBeGreaterThanOrEqual(foo.y + 20)
  })
})

describe('flattenPlaced', () => {
  it('emits parents before their children (paint order)', () => {
    expect(flattenPlaced(place()).map(label)).toEqual(['foo', 'a.txt', 'b.txt', 'bar'])
  })
})

describe('hitTest', () => {
  it('returns the deepest cell under a point (a leaf over its parent)', () => {
    const cells = place()
    const a = cells[0].children[0] // a.txt
    const cx = a.x + a.w / 2
    const cy = a.y + a.h / 2
    expect(hitTest(cells, cx, cy)).toBe(a)
  })

  it('returns the branch itself for a point in its title bar (no child there)', () => {
    const cells = place()
    const foo = cells[0]
    // 10px down the title bar — above where any child begins.
    expect(hitTest(cells, foo.x + foo.w / 2, foo.y + 10)).toBe(foo)
  })

  it('returns null outside every cell', () => {
    expect(hitTest(place(), W + 50, H + 50)).toBe(null)
  })
})
