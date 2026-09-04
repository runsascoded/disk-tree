import { describe, expect, it } from 'vitest'
import { edgeEmphFactor, flattenPlaced, hitTest, layoutCells, type LayoutConfig, type PlacedCell } from '../src/layout'
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
  edgeEmphasis: 0,
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

  it('area-descending sort keeps every parent before its children', () => {
    // The canvas renderer paints biggest-first; this is only a valid
    // ancestor-before-descendant order because a container's rect strictly
    // contains its descendants', so its area exceeds theirs.
    const cells = place()
    const flat = flattenPlaced(cells)
    flat.sort((a, b) => b.w * b.h - a.w * a.h)
    const idx = new Map(flat.map((c, i) => [c, i]))
    const check = (c: PlacedCell<N>) => {
      for (const k of c.children) {
        expect(idx.get(c)!).toBeLessThan(idx.get(k)!)
        check(k)
      }
    }
    cells.forEach(check)
  })
})

describe('edgeEmphFactor', () => {
  it('is 1 everywhere when emphasis is 0 (no change)', () => {
    expect([0, 1, 2, 3].map(d => edgeEmphFactor(d, 0))).toEqual([1, 1, 1, 1])
  })

  it('thickens shallow depths, decaying to 1 by depth 2', () => {
    // 1 + emphasis · max(0, 2 − depth)
    expect([0, 1, 2, 3].map(d => edgeEmphFactor(d, 1))).toEqual([3, 2, 1, 1])
    expect([0, 1, 2].map(d => edgeEmphFactor(d, 0.5))).toEqual([2, 1.5, 1])
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
