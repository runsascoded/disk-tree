import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render } from '@testing-library/react'
import { StalenessScatter } from '../src/StalenessScatter'
import { SEC_PER_YEAR, TB } from '../src/stats'

interface N {
  name: string
  age: number | null
  bytes: number | null
  files?: number
}

/** 1 TB @ 1y (score 1), 10 TB @ 0.1y (score 1), 0.1 TB @ 10y (score 1). */
const NODES: N[] = [
  { name: 'a', age: SEC_PER_YEAR, bytes: TB, files: 100 },
  { name: 'b', age: SEC_PER_YEAR / 10, bytes: 10 * TB, files: 400 },
  { name: 'c', age: SEC_PER_YEAR * 10, bytes: TB / 10, files: 25 },
]

const accessors = {
  getAge: (n: N) => n.age,
  getSize: (n: N) => n.bytes,
  getLabel: (n: N) => n.name,
}

function markers(container: HTMLElement) {
  return [...container.querySelectorAll('circle.dt-scatter-marker')]
}

describe('<StalenessScatter>', () => {
  it('renders one marker per plottable node, labeled by <title>', () => {
    const { container } = render(<StalenessScatter nodes={NODES} {...accessors} />)
    expect(markers(container).map(c => c.querySelector('title')?.textContent).sort()).toEqual(['a', 'b', 'c'])
  })

  it('places equal-score nodes on one straight, down-right line', () => {
    const { container } = render(<StalenessScatter nodes={NODES} {...accessors} />)
    const pts = markers(container).map(c => ({
      name: c.querySelector('title')!.textContent,
      x: Number(c.getAttribute('cx')),
      y: Number(c.getAttribute('cy')),
    }))
    const by = (n: string) => pts.find(p => p.name === n)!
    // All three score 1 TB·yr. b → a → c each step one decade older and one
    // decade smaller, so the pixel steps are equal and the three are
    // collinear. (The pixel slope is *not* −1: the plot box isn't square and
    // the axes span different decade counts — only the data-space slope is.)
    const dx1 = by('a').x - by('b').x
    const dy1 = by('a').y - by('b').y
    const dx2 = by('c').x - by('a').x
    const dy2 = by('c').y - by('a').y
    expect(Number((dx1 / dx2).toPrecision(6))).toBe(1)
    expect(Number((dy1 / dy2).toPrecision(6))).toBe(1)
    expect(Number((dx1 * dy2 - dx2 * dy1).toPrecision(6))).toBe(0)
    // Older is right (+x) and bigger is up (−y in SVG coords).
    expect([dx1 > 0, dy1 > 0]).toEqual([true, true])
  })

  it('scales marker area by weight, floored at the minimum radius', () => {
    const { container } = render(
      <StalenessScatter nodes={NODES} {...accessors} getWeight={n => n.files} />,
    )
    const r = Object.fromEntries(
      markers(container).map(c => [c.querySelector('title')!.textContent, Number(c.getAttribute('r'))]),
    )
    // files 400 / 100 / 25 → radii 14 / 7 / 3.5 (area ∝ files).
    expect([r.b, r.a, r.c]).toEqual([14, 7, 3.5])
  })

  it('uniform radius when no weight accessor is given', () => {
    const { container } = render(<StalenessScatter nodes={NODES} {...accessors} />)
    expect([...new Set(markers(container).map(c => c.getAttribute('r')))]).toEqual(['2.5'])
  })

  it('draws the diagonal the data actually sits on, labeled in TB·years', () => {
    // All three nodes score 1 TB·yr, so that is the one contour worth drawing
    // (the box corners would suggest 0.1 / 1 / 10, two of them through empty space).
    const { container } = render(<StalenessScatter nodes={NODES} {...accessors} />)
    const isoLabels = [...container.querySelectorAll('svg text')]
      .map(t => t.textContent!)
      .filter(t => t.endsWith('TB·yr'))
    expect(isoLabels).toEqual(['1 TB·yr'])
  })

  it('picks lower-unit labels for sub-TB·year scores', () => {
    const { container } = render(
      <StalenessScatter
        nodes={[
          { name: 'x', age: SEC_PER_YEAR, bytes: TB / 1e4 },
          { name: 'y', age: SEC_PER_YEAR * 100, bytes: TB / 1e2 },
        ]}
        {...accessors}
      />,
    )
    const isoLabels = [...container.querySelectorAll('svg text')]
      .map(t => t.textContent!)
      .filter(t => t.includes('·yr'))
    // Scores span 100 MB·yr … 1 TB·yr, so the interior decades are the three
    // GB·yr lines between them.
    expect(isoLabels).toEqual(['1 GB·yr', '10 GB·yr', '100 GB·yr'])
  })

  it('omits the diagonals when isoLines is false', () => {
    const { container } = render(<StalenessScatter nodes={NODES} {...accessors} isoLines={false} />)
    expect([...container.querySelectorAll('svg text')].filter(t => t.textContent!.endsWith('TB·yr'))).toEqual([])
  })

  it('shows label, size, age and score in the default tooltip on hover', () => {
    const { container } = render(<StalenessScatter nodes={NODES} {...accessors} />)
    fireEvent.mouseEnter(markers(container).find(c => c.querySelector('title')!.textContent === 'a')!)
    const tip = container.querySelector('.dt-scatter-tip')!
    expect([...tip.children].map(c => c.textContent)).toEqual(['a', '1 TB · 1y', '1 TB·yr'])
  })

  it('clears the tooltip on mouse leave', () => {
    const { container } = render(<StalenessScatter nodes={NODES} {...accessors} />)
    const a = markers(container).find(c => c.querySelector('title')!.textContent === 'a')!
    fireEvent.mouseEnter(a)
    fireEvent.mouseLeave(a)
    expect(container.querySelector('.dt-scatter-tip')).toBeNull()
  })

  it('calls onNodeClick with the clicked node', () => {
    const onNodeClick = vi.fn()
    const { container } = render(
      <StalenessScatter nodes={NODES} {...accessors} onNodeClick={onNodeClick} />,
    )
    fireEvent.click(markers(container).find(c => c.querySelector('title')!.textContent === 'c')!)
    expect(onNodeClick.mock.calls.map(([n]) => n.name)).toEqual(['c'])
  })

  it('counts unplottable nodes instead of dropping them silently', () => {
    const { container } = render(
      <StalenessScatter
        nodes={[...NODES, { name: 'no-age', age: null, bytes: TB }, { name: 'empty', age: SEC_PER_YEAR, bytes: 0 }]}
        {...accessors}
      />,
    )
    expect(markers(container).length).toBe(3)
    expect(container.querySelector('.dt-scatter-note')!.textContent).toBe('2 not plotted (no age or zero size)')
  })

  it('renders no plot and no note for an empty node list', () => {
    const { container } = render(<StalenessScatter nodes={[]} {...accessors} />)
    expect(container.querySelector('svg')).toBeNull()
    expect(container.querySelector('.dt-scatter-note')).toBeNull()
    expect(container.querySelector('.dt-scatter')).toBeInTheDocument()
  })
})
