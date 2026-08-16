import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render } from '@testing-library/react'
import { AgeHistograms } from '../src/AgeHistograms'

interface Item {
  name: string
  bins: number[]
}

const EDGES = [0, 100, 200, 300]
const ITEMS: Item[] = [
  { name: 'a', bins: [200, 0, 10] },
  { name: 'b', bins: [0, 60, 0] },
  { name: 'c', bins: [0, 0, 30] },
]

const accessors = {
  edges: EDGES,
  getBins: (i: Item) => i.bins,
  getLabel: (i: Item) => i.name,
}

const bars = (container: HTMLElement) => [...container.querySelectorAll('rect.dt-hist-bar')]

describe('<AgeHistograms>', () => {
  it('draws one bar per non-empty bin', () => {
    const { container } = render(<AgeHistograms items={ITEMS} {...accessors} />)
    expect(bars(container).length).toBe(4)
  })

  it('scales bar width by bytes against one shared peak, so area tracks bytes across items', () => {
    const { container } = render(<AgeHistograms items={ITEMS} {...accessors} />)
    const widths = bars(container).map(b => Number(b.getAttribute('width')))
    // Peak is a's 200-byte bin; b's 60 and c's 30 are 0.3× and 0.15× of it.
    const [wideA, thinA, wB, wC] = widths
    expect(Number((thinA / wideA).toPrecision(6))).toBe(0.05)
    expect(Number((wB / wideA).toPrecision(6))).toBe(0.3)
    expect(Number((wC / wideA).toPrecision(6))).toBe(0.15)
  })

  it('normalize scales each column to its own peak, trading area-∝-bytes for legible shape', () => {
    const { container } = render(<AgeHistograms items={ITEMS} {...accessors} normalize />)
    const widths = bars(container).map(b => Number(b.getAttribute('width')))
    // Every column's largest bin now reaches full width; a's small bin keeps
    // its within-column ratio (10/200).
    const [wideA, thinA, wB, wC] = widths
    expect([wB / wideA, wC / wideA]).toEqual([1, 1])
    expect(Number((thinA / wideA).toPrecision(6))).toBe(0.05)
  })

  it('puts older bins lower on screen', () => {
    const { container } = render(<AgeHistograms items={[ITEMS[0]]} {...accessors} />)
    const ys = bars(container).map(b => Number(b.getAttribute('y')))
    // a's bins are [oldest, _, newest]; the oldest must sit below the newest.
    expect(ys[0] > ys[1]).toBe(true)
  })

  it('labels each column', () => {
    const { container } = render(<AgeHistograms items={ITEMS} {...accessors} />)
    const labels = [...container.querySelectorAll('.dt-hist-col text')].map(t => t.textContent)
    expect(labels).toEqual(['a', 'b', 'c'])
  })

  it('reports the reclaimable total next to the threshold line', () => {
    const { container } = render(
      <AgeHistograms items={ITEMS} {...accessors} threshold={200} formatTime={t => `t=${t}`} />,
    )
    const label = container.querySelector('.dt-hist-threshold text')!
    // Bins fully below 200: a's 200 + b's 60 = 260 bytes.
    expect(label.textContent).toBe('260 B older than t=200')
  })

  it('omits threshold chrome when no threshold is given', () => {
    const { container } = render(<AgeHistograms items={ITEMS} {...accessors} />)
    expect(container.querySelector('.dt-hist-threshold')).toBeNull()
  })

  it('reports time and reclaimable bytes when the plot is clicked', () => {
    const onThresholdChange = vi.fn()
    const { container } = render(
      <AgeHistograms items={ITEMS} {...accessors} onThresholdChange={onThresholdChange} />,
    )
    const svg = container.querySelector('svg')!
    // jsdom has no layout, so getBoundingClientRect is all zeros: clientY 0
    // maps to the top of the plot, i.e. the newest edge → everything older.
    fireEvent.mouseDown(svg, { clientY: 0 })
    expect(onThresholdChange.mock.calls).toEqual([[300, 300]])
  })

  it('tracks the drag only while the button is held', () => {
    const onThresholdChange = vi.fn()
    const { container } = render(
      <AgeHistograms items={ITEMS} {...accessors} onThresholdChange={onThresholdChange} />,
    )
    const svg = container.querySelector('svg')!
    fireEvent.mouseMove(svg, { clientY: 0 })
    expect(onThresholdChange).not.toHaveBeenCalled()
    fireEvent.mouseDown(svg, { clientY: 0 })
    fireEvent.mouseMove(svg, { clientY: 0 })
    fireEvent.mouseUp(svg)
    fireEvent.mouseMove(svg, { clientY: 0 })
    expect(onThresholdChange.mock.calls.length).toBe(2)
  })

  it('shows an item total in the default tooltip on hover', () => {
    const { container } = render(<AgeHistograms items={ITEMS} {...accessors} />)
    fireEvent.mouseEnter(container.querySelectorAll('.dt-hist-col')[0])
    const tip = container.querySelector('.dt-hist-tip')!
    expect([...tip.children].map(c => c.textContent)).toEqual(['a', '210 B'])
  })

  it('adds the per-item reclaimable line to the tooltip when a threshold is set', () => {
    const { container } = render(<AgeHistograms items={ITEMS} {...accessors} threshold={150} />)
    fireEvent.mouseEnter(container.querySelectorAll('.dt-hist-col')[0])
    const tip = container.querySelector('.dt-hist-tip')!
    expect([...tip.children].map(c => c.textContent)).toEqual(['a', '210 B', '200 B older'])
  })

  it('calls onItemClick with the clicked item', () => {
    const onItemClick = vi.fn()
    const { container } = render(
      <AgeHistograms items={ITEMS} {...accessors} onItemClick={onItemClick} />,
    )
    fireEvent.click(container.querySelectorAll('.dt-hist-col')[1])
    expect(onItemClick.mock.calls.map(([i]) => i.name)).toEqual(['b'])
  })

  it('renders no plot for empty items or empty edges', () => {
    const { container: a } = render(<AgeHistograms items={[]} {...accessors} />)
    expect(a.querySelector('svg')).toBeNull()
    const { container: b } = render(<AgeHistograms items={ITEMS} {...accessors} edges={[]} getBins={() => []} />)
    expect(b.querySelector('svg')).toBeNull()
  })
})
